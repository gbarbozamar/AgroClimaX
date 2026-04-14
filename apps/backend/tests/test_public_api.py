import os
import tempfile
import unittest
import uuid
from datetime import date
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from PIL import Image

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.services.public_api import (
    OFFICIAL_MAP_OVERLAYS,
    TIMELINE_MANIFEST_CACHE,
    TIMELINE_SOURCE_CACHE,
    TRANSPARENT_PNG,
    _tile_content_is_good_enough,
    _coneat_cache_entry_paths,
    _coneat_cache_key,
    _is_persistable_timeline_source_metadata,
    _persist_timeline_source_metadata,
    _resolve_timeline_source_metadata,
    build_timeline_frame_manifest,
    list_official_map_overlays,
    proxy_coneat_request,
    proxy_official_overlay_tile,
    fetch_tile_png,
)
from app.services.raster_cache import viewport_bucket


class ConeatProxyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        TIMELINE_MANIFEST_CACHE.clear()
        TIMELINE_SOURCE_CACHE.clear()

    @staticmethod
    def _solid_png() -> bytes:
        buffer = BytesIO()
        Image.new("RGBA", (4, 4), (220, 80, 30, 255)).save(buffer, format="PNG")
        return buffer.getvalue()

    @staticmethod
    def _cloudlike_png(size: int = 256) -> bytes:
        buffer = BytesIO()
        Image.new("RGBA", (size, size), (236, 236, 236, 255)).save(buffer, format="PNG")
        return buffer.getvalue()

    def test_official_overlay_catalog_contains_first_wave_layers(self):
        items = list_official_map_overlays()
        overlay_ids = {item["id"] for item in items}
        self.assertTrue({"coneat", "hidrografia", "area_inundable", "catastro_rural", "rutas_camineria", "zonas_sensibles"}.issubset(overlay_ids))
        self.assertEqual(OFFICIAL_MAP_OVERLAYS["coneat"]["service_kind"], "arcgis_export")

    def test_bbox_normalization_generates_same_cache_key(self):
        params_precise = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": "-56.25000000000001,-31.952162238024975,-53.43750000000001,-29.535229562948455",
        }
        params_rounded = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": "-56.250000,-31.952162,-53.437500,-29.535230",
        }
        self.assertEqual(_coneat_cache_key(params_precise), _coneat_cache_key(params_rounded))

    async def test_coneat_getmap_delegates_to_official_overlay_proxy(self):
        params = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": "-56,-31,-55,-30",
            "WIDTH": "256",
            "HEIGHT": "256",
        }
        with patch(
            "app.services.public_api.proxy_official_overlay_tile",
            new=AsyncMock(return_value=(TRANSPARENT_PNG, "image/png")),
        ) as delegated:
            content, content_type = await proxy_coneat_request(params)

        delegated.assert_awaited_once()
        self.assertEqual(content, TRANSPARENT_PNG)
        self.assertEqual(content_type, "image/png")

    async def test_official_overlay_timeout_returns_transparent_png(self):
        params = {
            "bbox": f"-56,-31,-55,-30,{uuid.uuid4()}",
            "bboxSR": "4326",
            "imageSR": "4326",
            "width": 256,
            "height": 256,
            "format": "image/png",
            "transparent": "true",
        }
        with patch(
            "app.services.public_api._fetch_official_overlay_remote",
            new=AsyncMock(side_effect=httpx.ConnectTimeout("timeout")),
        ):
            content, content_type = await proxy_official_overlay_tile("hidrografia", params)
        self.assertEqual(content, TRANSPARENT_PNG)
        self.assertEqual(content_type, "image/png")

    async def test_getmap_compat_preserves_bbox_normalization_for_delegate(self):
        params = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": "-57.0000001,-32.0,-56.0000001,-31.0",
            "WIDTH": "512",
            "HEIGHT": "512",
        }
        with patch(
            "app.services.public_api.proxy_official_overlay_tile",
            new=AsyncMock(return_value=(b"cached-image", "image/png")),
        ) as delegated:
            content, content_type = await proxy_coneat_request(params)

        delegated.assert_awaited_once()
        delegated_params = delegated.await_args.args[1]
        self.assertEqual(delegated_params["bbox"], "-57.000000,-32.000000,-56.000000,-31.000000")
        self.assertEqual(content, b"cached-image")
        self.assertEqual(content_type, "image/png")

    async def test_timeline_manifest_requires_real_bucket_metadata_for_available_frames(self):
        persisted_index = {
            "ndmi": {
                "2026-04-02": {
                    "layer_id": "ndmi",
                    "availability": "runtime_bucket_probe",
                    "selection_reason": "runtime_bucket_probe",
                    "primary_source_date": "2026-04-01",
                    "secondary_source_date": "2026-04-06",
                    "blend_weight": 0.4,
                    "resolved_source_date": "2026-04-01",
                    "renderable_pixel_pct": 88.4,
                    "visual_empty": False,
                    "visual_state": "interpolated",
                    "source_locked": True,
                }
            }
        }
        with patch("app.services.public_api._load_persisted_timeline_source_metadata_index", new=AsyncMock(return_value=persisted_index)):
            with patch("app.services.public_api.get_canonical_product_status_index", new=AsyncMock(return_value={"ndmi": {}})):
                with patch(
                    "app.services.public_api._timeline_frame_cache_status_index",
                    new=AsyncMock(return_value={"ndmi": {"2026-04-02": "ready"}}),
                ):
                    payload = await build_timeline_frame_manifest(
                        layers=["ndmi"],
                        date_from=date(2026, 4, 1),
                        date_to=date(2026, 4, 3),
                        bbox="-56,-32,-55,-31",
                        zoom=11,
                        scope="unidad",
                        unit_id="unit-1",
                        scope_type="field",
                        scope_ref="field:1",
                    )
        self.assertEqual(payload["layers"], ["ndmi"])
        self.assertEqual(payload["total_days"], 3)
        day_payload = payload["days"][1]["layers"]["ndmi"]
        self.assertTrue(day_payload["available"])
        self.assertIn("primary_source_date", day_payload)
        self.assertIn("blend_weight", day_payload)
        self.assertEqual(day_payload["cache_status"], "ready")
        self.assertTrue(day_payload["warm_available"])
        self.assertIn("visual_state", day_payload)
        self.assertFalse(day_payload["skip_in_playback"])
        self.assertFalse(day_payload["visual_empty"])

    async def test_timeline_manifest_prefers_canonical_internal_product_index(self):
        with patch("app.services.public_api._load_timeline_snapshot_index", new=AsyncMock(return_value={"rgb": {}})):
            with patch("app.services.public_api._timeline_frame_cache_status_index", new=AsyncMock(return_value={"rgb": {"2026-04-02": "ready"}})):
                with patch(
                    "app.services.public_api.get_canonical_product_status_index",
                    new=AsyncMock(
                        return_value={
                            "rgb": {
                                "2026-04-02": {
                                    "layer_id": "rgb",
                                    "visual_state": "ready",
                                    "visual_empty": False,
                                    "renderable_pixel_pct": 94.5,
                                    "resolved_source_date": "2026-04-01",
                                    "coverage_origin": "department_daily_cog",
                                    "cache_status": "ready",
                                }
                            }
                        }
                    ),
                ):
                    payload = await build_timeline_frame_manifest(
                        layers=["rgb"],
                        date_from=date(2026, 4, 2),
                        date_to=date(2026, 4, 2),
                        bbox="-56,-32,-55,-31",
                        zoom=11,
                        scope="departamento",
                        department="Rivera",
                        scope_type="departamento",
                        scope_ref="Rivera",
                    )
        day_payload = payload["days"][0]["layers"]["rgb"]
        self.assertTrue(day_payload["available"])
        self.assertEqual(day_payload["coverage_origin"], "department_daily_cog")
        self.assertEqual(day_payload["visual_state"], "ready")
        self.assertEqual(day_payload["resolved_source_date"], "2026-04-01")

    async def test_timeline_manifest_marks_visual_empty_frames_as_non_playable(self):
        with patch("app.services.public_api._load_persisted_timeline_source_metadata_index", new=AsyncMock(return_value={"rgb": {}})):
            with patch(
                "app.services.public_api.get_canonical_product_status_index",
                new=AsyncMock(
                    return_value={
                        "rgb": {
                            "2026-04-02": {
                                "layer_id": "rgb",
                                "visual_state": "empty",
                                "visual_empty": True,
                                "renderable_pixel_pct": 2.0,
                                "empty_reason": "low_renderable_coverage",
                                "resolved_source_date": "2026-04-02",
                                "coverage_origin": "department_daily_cog",
                                "cache_status": "empty",
                            }
                        }
                    }
                ),
            ):
                with patch(
                    "app.services.public_api._timeline_frame_cache_status_index",
                    new=AsyncMock(return_value={"rgb": {"2026-04-02": "empty"}}),
                ):
                    payload = await build_timeline_frame_manifest(
                        layers=["rgb"],
                        date_from=date(2026, 4, 2),
                        date_to=date(2026, 4, 2),
                        bbox="-56,-32,-55,-31",
                        zoom=11,
                        scope="unidad",
                        unit_id="unit-1",
                        scope_type="field",
                        scope_ref="field:1",
                    )
        day_payload = payload["days"][0]["layers"]["rgb"]
        self.assertFalse(day_payload["available"])
        self.assertEqual(day_payload["visual_state"], "empty")
        self.assertTrue(day_payload["visual_empty"])
        self.assertTrue(day_payload["skip_in_playback"])
        self.assertEqual(day_payload["empty_reason"], "low_renderable_coverage")

    async def test_timeline_manifest_overrides_ready_frames_with_empty_cache_status(self):
        TIMELINE_MANIFEST_CACHE.clear()
        with patch(
            "app.services.public_api.get_canonical_product_status_index",
            new=AsyncMock(
                return_value={
                    "rgb": {
                        "2026-04-02": {
                            "layer_id": "rgb",
                            "visual_state": "ready",
                            "visual_empty": False,
                            "renderable_pixel_pct": 92.0,
                            "resolved_source_date": "2026-04-02",
                            "coverage_origin": "department_daily_cog",
                            "cache_status": "ready",
                        }
                    }
                }
            ),
        ):
            with patch("app.services.public_api._load_persisted_timeline_source_metadata_index", new=AsyncMock(return_value={"rgb": {}})):
                with patch(
                    "app.services.public_api._timeline_frame_cache_status_index",
                    new=AsyncMock(return_value={"rgb": {"2026-04-02": "empty"}}),
                ):
                    payload = await build_timeline_frame_manifest(
                        layers=["rgb"],
                        date_from=date(2026, 4, 2),
                        date_to=date(2026, 4, 2),
                        bbox="-56,-32,-55,-31",
                        zoom=11,
                        scope="unidad",
                        unit_id="unit-1",
                        scope_type="field",
                        scope_ref="field:1",
                    )
        day_payload = payload["days"][0]["layers"]["rgb"]
        self.assertFalse(day_payload["available"])
        self.assertEqual(day_payload["visual_state"], "empty")
        self.assertTrue(day_payload["visual_empty"])
        self.assertTrue(day_payload["skip_in_playback"])
        self.assertEqual(day_payload["empty_reason"], "warm_cache_empty")

    async def test_timeline_source_metadata_prefers_scope_specific_snapshot(self):
        TIMELINE_SOURCE_CACHE.clear()
        snapshot_index = {
            "rgb": {
                date(2026, 4, 2): [
                    {
                        "observed_date": date(2026, 4, 2),
                        "metadata_extra": {
                            "primary_source_date": "2026-04-01",
                            "availability": "available",
                            "renderable_pixel_pct": 87.0,
                            "scope_ref": "field:1",
                        },
                        "availability_score": 0.8,
                        "unit_id": "unit-1",
                        "scope": "unidad",
                        "department": "Rivera",
                    },
                    {
                        "observed_date": date(2026, 4, 2),
                        "metadata_extra": {
                            "primary_source_date": "2026-03-30",
                            "availability": "available",
                            "renderable_pixel_pct": 90.0,
                            "scope_ref": "field:2",
                        },
                        "availability_score": 0.95,
                        "unit_id": "unit-2",
                        "scope": "unidad",
                        "department": "Tacuarembo",
                    },
                ]
            }
        }
        with patch("app.services.public_api._load_timeline_snapshot_index", new=AsyncMock(return_value=snapshot_index)):
            payload = await _resolve_timeline_source_metadata(
                "rgb",
                date(2026, 4, 2),
                bbox_bucket="-56.00,-32.00,-55.00,-31.00",
                scope="unidad",
                unit_id="unit-1",
                scope_type="field",
                scope_ref="field:1",
            )
        self.assertEqual(payload["primary_source_date"], "2026-04-01")
        self.assertEqual(payload["selection_reason"], "snapshot_exact")
        self.assertFalse(payload["visual_empty"])
        self.assertTrue(payload["source_locked"])
        self.assertEqual(payload["resolved_source_date"], "2026-04-01")

    async def test_timeline_source_metadata_carries_forward_previous_runtime_probe_for_cloud_layers(self):
        TIMELINE_SOURCE_CACHE.clear()

        async def _fake_probe(*, display_date, **kwargs):
            if display_date == date(2026, 4, 7):
                return None
            if display_date == date(2026, 4, 6):
                return {
                    "layer_id": "rgb",
                    "available": True,
                    "availability": "runtime_bucket_probe",
                    "is_interpolated": True,
                    "primary_source_date": "2026-03-27",
                    "secondary_source_date": None,
                    "blend_weight": 0.0,
                    "label": "Interpolado",
                    "valid_pixel_pct": 100.0,
                    "cloud_pixel_pct": 0.0,
                    "renderable_pixel_pct": 100.0,
                    "visual_empty": False,
                    "visual_state": "interpolated",
                    "skip_in_playback": False,
                    "empty_reason": None,
                    "selection_reason": "runtime_bucket_probe",
                    "coverage_origin": "runtime_bucket_probe",
                    "resolved_source_date": "2026-03-27",
                    "resolved_from_cache": False,
                    "source_locked": True,
                    "fusion_mode": None,
                    "s1_present": False,
                    "s2_present": True,
                    "s2_mask_valid": True,
                }
            return None

        with patch("app.services.public_api._load_timeline_snapshot_index", new=AsyncMock(return_value={"rgb": {}})):
            with patch("app.services.public_api._load_persisted_timeline_source_metadata", new=AsyncMock(return_value=None)):
                with patch("app.services.public_api._persist_timeline_source_metadata", new=AsyncMock()):
                    with patch("app.services.public_api._probe_runtime_bucket_source_metadata", new=AsyncMock(side_effect=_fake_probe)):
                        payload = await _resolve_timeline_source_metadata(
                            "rgb",
                            date(2026, 4, 7),
                            bbox_bucket="-59.750,-35.250,-52.250,-30.250",
                            bbox="-59.8206,-35.1648,-52.1741,-30.3634",
                            zoom=7,
                            scope="nacional",
                            scope_type="nacional",
                            scope_ref="Uruguay",
                            allow_runtime_probe=True,
                        )

        self.assertTrue(payload["available"])
        self.assertEqual(payload["selection_reason"], "runtime_bucket_carry_forward")
        self.assertEqual(payload["resolved_source_date"], "2026-03-27")
        self.assertEqual(payload["primary_source_date"], "2026-03-27")
        self.assertEqual(payload["carry_forward_from_display_date"], "2026-04-06")
        self.assertFalse(payload["visual_empty"])

    def test_timeline_source_metadata_rejects_runtime_tile_unlock_fallback_for_persistence(self):
        self.assertFalse(
            _is_persistable_timeline_source_metadata(
                {
                    "selection_reason": "runtime_bucket_carry_forward",
                    "coverage_origin": "runtime_tile_unlock_fallback",
                    "resolved_source_date": "2026-04-03",
                }
            )
        )
        self.assertFalse(
            _is_persistable_timeline_source_metadata(
                {
                    "selection_reason": "runtime_bucket_probe",
                    "coverage_origin": "runtime_bucket_probe",
                    "resolved_source_date": "2026-03-27",
                }
            )
        )
        self.assertTrue(
            _is_persistable_timeline_source_metadata(
                {
                    "selection_reason": "runtime_bucket_probe",
                    "coverage_origin": "runtime_bucket_probe",
                    "resolved_source_date": "2026-03-27",
                    "probe_version": 2,
                    "good_tile_ratio": 0.8889,
                }
            )
        )

    async def test_persist_timeline_source_metadata_skips_runtime_tile_unlock_fallback(self):
        with patch("app.services.public_api.upsert_raster_cache_entry", new=AsyncMock()) as upsert:
            await _persist_timeline_source_metadata(
                layer="rgb",
                display_date=date(2026, 4, 8),
                bbox_bucket="-58.500,-35.000,-53.250,-30.000",
                scope_type="nacional",
                scope_ref="Uruguay",
                metadata={
                    "selection_reason": "runtime_bucket_carry_forward",
                    "coverage_origin": "runtime_tile_unlock_fallback",
                    "resolved_source_date": "2026-03-29",
                },
            )
        upsert.assert_not_awaited()

    async def test_temporal_tile_prefers_viewport_raster_product_before_remote(self):
        product_png = self._solid_png()
        with patch(
            "app.services.public_api.read_viewport_raster_product_tile",
            new=AsyncMock(return_value=(product_png, {"source_date": "2026-04-04", "coverage_origin": "viewport_bucket_product"})),
        ):
            with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock()) as resolver:
                with patch("app.services.public_api.requests.post") as remote_post:
                    content = await fetch_tile_png(
                        "rgb",
                        7,
                        43,
                        77,
                        target_date=date(2026, 4, 4),
                        frame_role="primary",
                    )
        self.assertEqual(content, product_png)
        resolver.assert_not_called()
        remote_post.assert_not_called()

    async def test_temporal_tile_uses_scope_viewport_product_fallback_when_exact_bucket_is_missing(self):
        product_png = self._solid_png()
        with patch(
            "app.services.public_api.fetch_tileserver_tile",
            new=AsyncMock(return_value=(None, None)),
        ):
            with patch(
                "app.services.public_api.render_canonical_raster_tile",
                new=AsyncMock(return_value=(None, None)),
            ):
                with patch(
                    "app.services.public_api.read_viewport_raster_product_tile",
                    new=AsyncMock(return_value=(None, None)),
                ):
                    with patch(
                        "app.services.public_api.read_scope_viewport_raster_fallback_tile",
                        new=AsyncMock(return_value=(product_png, {"source_date": "2026-04-04", "coverage_origin": "viewport_bucket_mosaic_fallback"})),
                    ) as fallback_reader:
                        with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock()) as resolver:
                            with patch("app.services.public_api.requests.post") as remote_post:
                                content = await fetch_tile_png(
                                    "alerta_fusion",
                                    7,
                                    43,
                                    76,
                                    target_date=date(2026, 4, 8),
                                    scope="nacional",
                                    scope_type="nacional",
                                    scope_ref="Uruguay",
                                    use_internal_products=True,
                                )
        self.assertEqual(content, product_png)
        resolver.assert_not_called()
        remote_post.assert_not_called()
        fallback_reader.assert_awaited()

    async def test_temporal_tile_internal_only_cutover_skips_remote_fallback(self):
        with patch(
            "app.services.public_api.fetch_tileserver_tile",
            new=AsyncMock(return_value=(None, None)),
        ):
            with patch(
                "app.services.public_api.render_canonical_raster_tile",
                new=AsyncMock(return_value=(None, None)),
            ):
                with patch(
                    "app.services.public_api.read_viewport_raster_product_tile",
                    new=AsyncMock(return_value=(None, None)),
                ):
                    with patch(
                        "app.services.public_api.read_scope_viewport_raster_fallback_tile",
                        new=AsyncMock(return_value=(None, None)),
                    ):
                        with patch(
                            "app.services.public_api.settings",
                            new=SimpleNamespace(
                                copernicus_enabled=True,
                                serve_tiles_internal=True,
                                internal_only_layers=["rgb"],
                                internal_only_scopes=["nacional"],
                            ),
                        ):
                            with patch(
                                "app.services.public_api._resolve_timeline_source_metadata",
                                new=AsyncMock(
                                    return_value={
                                        "layer_id": "rgb",
                                        "available": True,
                                        "availability": "available",
                                        "primary_source_date": "2026-04-04",
                                        "resolved_source_date": "2026-04-04",
                                        "visual_empty": False,
                                        "visual_state": "ready",
                                        "selection_reason": "snapshot_exact",
                                        "source_locked": True,
                                    }
                                ),
                            ):
                                with patch("app.services.public_api.requests.post") as remote_post:
                                    content = await fetch_tile_png(
                                        "rgb",
                                        7,
                                        43,
                                        77,
                                        target_date=date(2026, 4, 4),
                                        scope="nacional",
                                        scope_type="nacional",
                                        scope_ref="Uruguay",
                                    )
        self.assertEqual(content, TRANSPARENT_PNG)
        remote_post.assert_not_called()

    async def test_temporal_tile_disable_heuristic_ready_blocks_remote_unlock(self):
        with patch("app.services.public_api.fetch_tileserver_tile", new=AsyncMock(return_value=(None, None))):
            with patch("app.services.public_api.render_canonical_raster_tile", new=AsyncMock(return_value=(None, None))):
                with patch("app.services.public_api.read_viewport_raster_product_tile", new=AsyncMock(return_value=(None, None))):
                    with patch(
                        "app.services.public_api.settings",
                        new=SimpleNamespace(
                            copernicus_enabled=True,
                            serve_tiles_internal=False,
                            disable_heuristic_ready=True,
                        ),
                    ):
                        with patch(
                            "app.services.public_api._resolve_timeline_source_metadata",
                            new=AsyncMock(
                                return_value={
                                    "layer_id": "rgb",
                                    "available": True,
                                    "availability": "heuristic_fallback",
                                    "primary_source_date": "2026-04-04",
                                    "resolved_source_date": None,
                                    "visual_empty": False,
                                    "visual_state": "ready",
                                    "selection_reason": "heuristic_fallback",
                                    "source_locked": False,
                                }
                            ),
                        ):
                            with patch("app.services.public_api.requests.post") as remote_post:
                                content = await fetch_tile_png(
                                    "rgb",
                                    7,
                                    43,
                                    77,
                                    target_date=date(2026, 4, 4),
                                    viewport_bbox="-56,-32,-55,-31",
                                    viewport_zoom=7,
                                )
        self.assertEqual(content, TRANSPARENT_PNG)
        remote_post.assert_not_called()

    async def test_temporal_tile_skips_cloudlike_internal_rgb_product_and_falls_back_remote(self):
        class _FakeResponse:
            status_code = 200
            headers = {"content-type": "image/png"}
            content = ConeatProxyTests._solid_png()

        with tempfile.TemporaryDirectory() as temp_tile_cache:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_tile_cache)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch(
                        "app.services.public_api.read_viewport_raster_product_tile",
                        new=AsyncMock(return_value=(self._cloudlike_png(), {"source_date": "2026-04-04", "coverage_origin": "viewport_bucket_product"})),
                    ):
                        with patch("app.services.public_api.fetch_tileserver_tile", new=AsyncMock(return_value=(None, None))):
                            with patch("app.services.public_api.render_canonical_raster_tile", new=AsyncMock(return_value=(None, None))):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(return_value={
                                    "layer_id": "rgb",
                                    "available": True,
                                    "availability": "heuristic_fallback",
                                    "is_interpolated": False,
                                    "primary_source_date": "2026-04-04",
                                    "secondary_source_date": None,
                                    "blend_weight": 0.0,
                                    "label": "Real",
                                    "valid_pixel_pct": 100.0,
                                    "cloud_pixel_pct": 0.0,
                                    "renderable_pixel_pct": 100.0,
                                    "visual_empty": False,
                                    "visual_state": "ready",
                                    "skip_in_playback": False,
                                    "empty_reason": None,
                                    "selection_reason": "heuristic_fallback",
                                    "resolved_source_date": "2026-04-04",
                                    "source_locked": False,
                                    "fusion_mode": None,
                                    "s1_present": False,
                                    "s2_present": True,
                                    "s2_mask_valid": True,
                                })):
                                    with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                        with patch("app.services.public_api.requests.post", return_value=_FakeResponse()) as remote_post:
                                            content = await fetch_tile_png(
                                                "rgb",
                                                7,
                                                43,
                                                77,
                                                target_date=date(2026, 4, 4),
                                                viewport_bbox="-56,-32,-55,-31",
                                                viewport_zoom=7,
                                                use_internal_products=True,
                                            )
        self.assertEqual(content, _FakeResponse.content)
        remote_post.assert_called()

    async def test_temporal_tile_uses_resolved_timeline_source_window(self):
        class _FakeResponse:
            status_code = 200
            headers = {"content-type": "image/png"}
            content = b"png"

        captured: dict[str, object] = {}

        def _fake_post(url, json=None, headers=None, timeout=None):
            captured["payload"] = json
            return _FakeResponse()

        async def _fake_resolve(layer, display_date, **kwargs):
            return {
                "layer_id": "ndmi",
                "available": True,
                "availability": "available",
                "is_interpolated": False,
                "primary_source_date": "2025-12-24",
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Real",
                "valid_pixel_pct": 91.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 91.0,
                "visual_empty": False,
                "visual_state": "ready",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "snapshot_exact",
                "coverage_origin": "snapshot_exact",
                "resolved_source_date": "2025-12-24",
                "resolved_from_cache": False,
                "source_locked": True,
                "fusion_mode": None,
                "s1_present": False,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                        content = await fetch_tile_png(
                                            "ndmi",
                                            8,
                                            100,
                                            120,
                                            target_date=date(2025, 12, 24),
                                            frame_role="primary",
                                        )

        self.assertEqual(content, b"png")
        payload = captured["payload"]
        time_range = payload["input"]["data"][0]["dataFilter"]["timeRange"]
        self.assertEqual(time_range["from"], "2025-12-22T00:00:00Z")
        self.assertEqual(time_range["to"], "2025-12-26T23:59:59Z")
        self.assertEqual(payload["metadata"]["display_date"], "2025-12-24")
        self.assertEqual(payload["metadata"]["source_date"], "2025-12-24")

    async def test_temporal_tile_uses_viewport_bucket_when_provided(self):
        class _FakeResponse:
            status_code = 200
            headers = {"content-type": "image/png"}
            content = b"png"

        captured: dict[str, object] = {}

        async def _fake_resolve(layer, display_date, **kwargs):
            captured["bbox_bucket"] = kwargs.get("bbox_bucket")
            return {
                "layer_id": "rgb",
                "available": True,
                "availability": "available",
                "is_interpolated": False,
                "primary_source_date": "2026-04-04",
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Real",
                "valid_pixel_pct": 87.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 87.0,
                "visual_empty": False,
                "visual_state": "ready",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "snapshot_exact",
                "fusion_mode": None,
                "s1_present": False,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api.requests.post", return_value=_FakeResponse()):
                                        content = await fetch_tile_png(
                                            "rgb",
                                            7,
                                            43,
                                            76,
                                            target_date=date(2026, 4, 4),
                                            viewport_bbox="-61.9519,-35.1648,-50.0427,-30.3634",
                                            viewport_zoom=7,
                                        )

        self.assertEqual(content, b"png")
        self.assertEqual(
            captured["bbox_bucket"],
            viewport_bucket("-61.9519,-35.1648,-50.0427,-30.3634", zoom=7),
        )

    async def test_temporal_tile_retries_candidate_date_when_first_response_is_empty(self):
        class _FakeResponse:
            def __init__(self, content: bytes):
                self.status_code = 200
                self.headers = {"content-type": "image/png"}
                self.content = content

        call_source_dates: list[str] = []
        visible_png = self._solid_png()

        async def _fake_resolve(layer, display_date, **kwargs):
            return {
                "layer_id": "rgb",
                "available": True,
                "availability": "heuristic_fallback",
                "is_interpolated": True,
                "primary_source_date": "2026-04-01",
                "secondary_source_date": "2026-04-06",
                "blend_weight": 0.4,
                "label": "Interpolado",
                "valid_pixel_pct": 35.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 35.0,
                "visual_empty": False,
                "visual_state": "interpolated",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "heuristic_fallback",
                "fusion_mode": None,
                "s1_present": False,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        def _fake_post(*args, **kwargs):
            payload = kwargs.get("json") or {}
            metadata = payload.get("metadata") or {}
            call_source_dates.append(str(metadata.get("source_date")))
            if len(call_source_dates) == 1:
                return _FakeResponse(TRANSPARENT_PNG)
            return _FakeResponse(visible_png)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                        content = await fetch_tile_png(
                                            "rgb",
                                            7,
                                            43,
                                            76,
                                            target_date=date(2026, 4, 4),
                                            frame_role="primary",
                                        )

        self.assertEqual(content, visible_png)
        self.assertGreaterEqual(len(call_source_dates), 2)
        self.assertEqual(call_source_dates[0], "2026-04-04")

    async def test_temporal_tile_keeps_snapshot_exact_source_date_locked(self):
        class _FakeResponse:
            def __init__(self, content: bytes):
                self.status_code = 200
                self.headers = {"content-type": "image/png"}
                self.content = content

        call_source_dates: list[str] = []
        visible_png = self._solid_png()

        async def _fake_resolve(layer, display_date, **kwargs):
            return {
                "layer_id": "rgb",
                "available": True,
                "availability": "available",
                "is_interpolated": False,
                "primary_source_date": "2026-04-04",
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Real",
                "valid_pixel_pct": 100.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 100.0,
                "visual_empty": False,
                "visual_state": "ready",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "snapshot_exact",
                "resolved_source_date": "2026-04-04",
                "source_locked": True,
                "fusion_mode": None,
                "s1_present": False,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        def _fake_post(*args, **kwargs):
            payload = kwargs.get("json") or {}
            metadata = payload.get("metadata") or {}
            call_source_dates.append(str(metadata.get("source_date")))
            if len(call_source_dates) == 1:
                return _FakeResponse(TRANSPARENT_PNG)
            return _FakeResponse(visible_png)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                        content = await fetch_tile_png(
                                            "rgb",
                                            7,
                                            43,
                                            76,
                                            target_date=date(2026, 4, 4),
                                            frame_role="primary",
                                        )

        self.assertEqual(content, visible_png)
        self.assertGreaterEqual(len(call_source_dates), 2)
        self.assertEqual(set(call_source_dates), {"2026-04-04"})

    def test_rgb_cloudlike_tile_is_not_good_enough(self):
        self.assertFalse(_tile_content_is_good_enough(self._cloudlike_png(), layer="rgb"))

    def test_rgb_transparent_tile_is_not_good_enough(self):
        self.assertFalse(_tile_content_is_good_enough(TRANSPARENT_PNG, layer="rgb"))

    async def test_temporal_tile_unlocks_rgb_source_date_when_locked_tile_is_cloudlike(self):
        class _FakeResponse:
            def __init__(self, content: bytes):
                self.status_code = 200
                self.headers = {"content-type": "image/png"}
                self.content = content

        call_source_dates: list[str] = []
        cloudy_png = self._cloudlike_png()
        visible_png = self._solid_png()

        async def _fake_resolve(layer, display_date, **kwargs):
            return {
                "layer_id": "rgb",
                "available": True,
                "availability": "available",
                "is_interpolated": False,
                "primary_source_date": "2026-04-04",
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Real",
                "valid_pixel_pct": 100.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 100.0,
                "visual_empty": False,
                "visual_state": "ready",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "snapshot_exact",
                "resolved_source_date": "2026-04-04",
                "source_locked": True,
                "fusion_mode": None,
                "s1_present": False,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        def _fake_post(*args, **kwargs):
            payload = kwargs.get("json") or {}
            metadata = payload.get("metadata") or {}
            requested_date = str(metadata.get("source_date"))
            call_source_dates.append(requested_date)
            if requested_date == "2026-04-04":
                return _FakeResponse(cloudy_png)
            return _FakeResponse(visible_png)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api._persist_timeline_source_metadata", new=AsyncMock()) as persisted:
                                        with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                            content = await fetch_tile_png(
                                                "rgb",
                                                7,
                                                43,
                                                76,
                                                target_date=date(2026, 4, 4),
                                                frame_role="primary",
                                                viewport_bbox="-56,-32,-55,-31",
                                                viewport_zoom=7,
                                                scope="departamento",
                                                department="Rivera",
                                                scope_type="departamento",
                                                scope_ref="Rivera",
                                            )

        self.assertEqual(content, visible_png)
        self.assertIn("2026-04-04", call_source_dates)
        self.assertGreaterEqual(len(set(call_source_dates)), 2)
        persisted.assert_awaited()

    async def test_temporal_tile_keeps_manifest_locked_source_when_frame_signature_is_present(self):
        class _FakeResponse:
            def __init__(self, content: bytes):
                self.status_code = 200
                self.headers = {"content-type": "image/png"}
                self.content = content

        call_source_dates: list[str] = []
        cloudy_png = self._cloudlike_png()

        async def _fake_resolve(layer, display_date, **kwargs):
            return {
                "layer_id": "rgb",
                "display_date": "2026-04-04",
                "available": True,
                "availability": "available",
                "is_interpolated": False,
                "primary_source_date": "2026-04-04",
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Real",
                "valid_pixel_pct": 100.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 100.0,
                "visual_empty": False,
                "visual_state": "ready",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "snapshot_exact",
                "resolved_source_date": "2026-04-04",
                "source_locked": True,
                "frame_signature": "locked-frame-1",
                "fusion_mode": None,
                "s1_present": False,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        def _fake_post(*args, **kwargs):
            payload = kwargs.get("json") or {}
            metadata = payload.get("metadata") or {}
            requested_date = str(metadata.get("source_date"))
            call_source_dates.append(requested_date)
            return _FakeResponse(cloudy_png)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                        content = await fetch_tile_png(
                                            "rgb",
                                            7,
                                            43,
                                            76,
                                            target_date=date(2026, 4, 4),
                                            requested_source_date=date(2026, 4, 4),
                                            frame_role="primary",
                                            frame_signature="locked-frame-1",
                                        )

        self.assertEqual(content, TRANSPARENT_PNG)
        self.assertEqual(call_source_dates, ["2026-04-04"])

    async def test_alerta_tile_keeps_locked_source_date_when_retrying_widened_window(self):
        class _FakeResponse:
            def __init__(self, content: bytes):
                self.status_code = 200
                self.headers = {"content-type": "image/png"}
                self.content = content

        sparse_png = BytesIO()
        sparse_image = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
        for offset in range(4):
            sparse_image.putpixel((offset, offset), (231, 76, 60, 255))
        sparse_image.save(sparse_png, format="PNG")

        call_source_dates: list[str] = []
        visible_png = self._solid_png()

        async def _fake_resolve(layer, display_date, **kwargs):
            return {
                "layer_id": "alerta",
                "available": True,
                "availability": "available",
                "is_interpolated": False,
                "primary_source_date": "2026-04-04",
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Real",
                "valid_pixel_pct": 100.0,
                "cloud_pixel_pct": 0.0,
                "renderable_pixel_pct": 100.0,
                "visual_empty": False,
                "visual_state": "ready",
                "skip_in_playback": False,
                "empty_reason": None,
                "selection_reason": "snapshot_exact",
                "resolved_source_date": "2026-04-04",
                "source_locked": True,
                "fusion_mode": "s1_s2",
                "s1_present": True,
                "s2_present": True,
                "s2_mask_valid": True,
            }

        def _fake_post(*args, **kwargs):
            payload = kwargs.get("json") or {}
            metadata = payload.get("metadata") or {}
            call_source_dates.append(str(metadata.get("source_date")))
            if len(call_source_dates) == 1:
                return _FakeResponse(sparse_png.getvalue())
            return _FakeResponse(visible_png)

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api._resolve_timeline_source_metadata", new=AsyncMock(side_effect=_fake_resolve)):
                                    with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                        content = await fetch_tile_png(
                                            "alerta_fusion",
                                            7,
                                            43,
                                            77,
                                            target_date=date(2026, 4, 4),
                                            frame_role="primary",
                                        )

        self.assertEqual(content, visible_png)
        self.assertGreaterEqual(len(call_source_dates), 2)
        self.assertEqual(call_source_dates[0], "2026-04-04")


if __name__ == "__main__":
    unittest.main()
