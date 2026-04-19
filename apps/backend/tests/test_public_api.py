import os
import tempfile
import unittest
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.services.public_api import (
    OFFICIAL_MAP_OVERLAYS,
    TRANSPARENT_PNG,
    _coneat_cache_entry_paths,
    _coneat_cache_key,
    build_timeline_frame_manifest,
    list_official_map_overlays,
    proxy_coneat_request,
    proxy_official_overlay_tile,
    fetch_tile_png,
)


class ConeatProxyTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_timeline_manifest_marks_ndmi_as_interpolated_between_anchor_days(self):
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
            )
        self.assertEqual(payload["layers"], ["ndmi"])
        self.assertEqual(payload["total_days"], 3)
        day_payload = payload["days"][1]["layers"]["ndmi"]
        self.assertTrue(day_payload["available"])
        self.assertIn("primary_source_date", day_payload)
        self.assertIn("blend_weight", day_payload)
        self.assertEqual(day_payload["cache_status"], "ready")
        self.assertTrue(day_payload["warm_available"])

    async def test_temporal_tile_uses_resolved_timeline_source_window(self):
        # Must be >= 1 KB with a valid PNG signature so fetch_tile_png caches it
        # instead of rejecting it as a suspicious placeholder.
        _valid_png = b"\x89PNG\r\n\x1a\n" + b"\xAA" * 2048

        class _FakeResponse:
            status_code = 200
            headers = {"content-type": "image/png"}
            content = _valid_png

        captured: dict[str, object] = {}

        def _fake_post(url, json=None, headers=None, timeout=None):
            captured["payload"] = json
            return _FakeResponse()

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("app.services.public_api.TILE_CACHE_DIR", Path(temp_dir)):
                with patch("app.services.public_api.storage_get_bytes", new=AsyncMock(return_value=None)):
                    with patch("app.services.public_api.storage_put_bytes", new=AsyncMock()):
                        with patch("app.services.public_api.settings", new=SimpleNamespace(copernicus_enabled=True)):
                            with patch("app.services.public_api.legacy_get_token", return_value="token"):
                                with patch("app.services.public_api.requests.post", side_effect=_fake_post):
                                    content = await fetch_tile_png(
                                        "ndmi",
                                        8,
                                        100,
                                        120,
                                        target_date=date(2025, 12, 24),
                                        frame_role="primary",
                                    )

        self.assertEqual(content, _valid_png)
        payload = captured["payload"]
        time_range = payload["input"]["data"][0]["dataFilter"]["timeRange"]
        self.assertEqual(time_range["from"], "2025-12-20T00:00:00Z")
        self.assertEqual(time_range["to"], "2025-12-24T23:59:59Z")
        self.assertEqual(payload["metadata"]["display_date"], "2025-12-24")
        self.assertEqual(payload["metadata"]["source_date"], "2025-12-22")


if __name__ == "__main__":
    unittest.main()
