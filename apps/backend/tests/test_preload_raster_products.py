import os
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

TEST_DB = Path(__file__).resolve().parent / f"test_preload_raster_products_{uuid.uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["DATABASE_USE_POSTGIS"] = "false"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["TEMPORAL_PREWARM_ENABLED"] = "false"
os.environ["PRELOAD_NEIGHBOR_DAYS"] = "0"
os.environ["PRELOAD_ADJACENT_ZOOM_DELTA"] = "0"

from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal, Base, engine
from app.models.materialized import RasterCacheEntry
from app.services.preload import PRELOAD_TASKS, _create_and_schedule_run, _execute_preload_run, _mark_temporal_cache_ready, _preload_run_signature, _sample_tile_coords, get_preload_status
from app.services.raster_cache import create_preload_run, raster_cache_key, viewport_bucket
from app.services.raster_products import _asset_intersects_tile, _lat_to_tile_y, _lon_to_tile_x


class PreloadSamplingTests(unittest.TestCase):
    def test_sample_tile_coords_spreads_across_bbox(self):
        coords = [(x, y) for x in range(10) for y in range(10)]

        sampled = _sample_tile_coords(coords, max_tiles=9)

        self.assertLessEqual(len(sampled), 9)
        self.assertIn((0, 0), sampled)
        self.assertGreaterEqual(max(x for x, _ in sampled), 8)
        self.assertGreaterEqual(max(y for _, y in sampled), 8)


class NationalMosaicFilteringTests(unittest.TestCase):
    def test_asset_intersects_tile_uses_bbox_filter(self):
        asset = {"bbox": "-58.0,-35.0,-57.0,-34.0"}
        inside_x = _lon_to_tile_x(-57.5, 7)
        inside_y = _lat_to_tile_y(-34.5, 7)
        outside_x = _lon_to_tile_x(-55.0, 7)
        outside_y = _lat_to_tile_y(-31.0, 7)

        self.assertTrue(_asset_intersects_tile(asset, x=inside_x, y=inside_y, z=7))
        self.assertFalse(_asset_intersects_tile(asset, x=outside_x, y=outside_y, z=7))


class TemporalCacheEntryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await engine.dispose()

    async def test_mark_temporal_cache_ready_persists_empty_status(self):
        await _mark_temporal_cache_ready(
            layer_id="rgb",
            display_date=date(2026, 4, 2),
            source_date="2026-04-02",
            zoom=11,
            bbox="-56,-32,-55,-31",
            scope_type="nacional",
            scope_ref="Uruguay",
            bytes_size=123,
            frame_metadata={"visual_empty": True, "visual_state": "empty", "renderable_pixel_pct": 0.0, "empty_reason": "missing_snapshot"},
        )
        bucket = viewport_bucket("-56,-32,-55,-31", zoom=11)
        cache_key = raster_cache_key(
            cache_kind="analytic_tile",
            layer_id="rgb",
            display_date=date(2026, 4, 2),
            source_date="2026-04-02",
            zoom=11,
            bbox_bucket=bucket,
            scope_type="nacional",
            scope_ref="Uruguay",
        )
        async with AsyncSessionLocal() as session:
            row = (await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key))).scalar_one()
        self.assertEqual(row.status, "empty")
        self.assertTrue(row.metadata_extra.get("visual_empty"))
        self.assertEqual(row.metadata_extra.get("visual_state"), "empty")
        self.assertEqual(row.metadata_extra.get("empty_reason"), "missing_snapshot")

    async def test_mark_temporal_cache_ready_persists_ready_status(self):
        await _mark_temporal_cache_ready(
            layer_id="rgb",
            display_date=date(2026, 4, 3),
            source_date="2026-04-03",
            zoom=11,
            bbox="-56,-32,-55,-31",
            scope_type="nacional",
            scope_ref="Uruguay",
            bytes_size=456,
            frame_metadata={"visual_empty": False, "visual_state": "ready", "renderable_pixel_pct": 88.0},
        )
        bucket = viewport_bucket("-56,-32,-55,-31", zoom=11)
        cache_key = raster_cache_key(
            cache_kind="analytic_tile",
            layer_id="rgb",
            display_date=date(2026, 4, 3),
            source_date="2026-04-03",
            zoom=11,
            bbox_bucket=bucket,
            scope_type="nacional",
            scope_ref="Uruguay",
        )
        async with AsyncSessionLocal() as session:
            row = (await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key))).scalar_one()
        self.assertEqual(row.status, "ready")
        self.assertFalse(row.metadata_extra.get("visual_empty"))
        self.assertEqual(row.metadata_extra.get("visual_state"), "ready")


class PreloadViewportProductTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_preload_run_materializes_viewport_product_when_full_sampling(self):
        bbox = "-56.0,-31.0,-55.9,-30.9"
        target = date(2026, 4, 4)
        run_key = "test-run-key"

        source_metadata = {
            "layer_id": "rgb",
            "available": True,
            "availability": "available",
            "is_interpolated": False,
            "primary_source_date": target.isoformat(),
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
            "resolved_source_date": target.isoformat(),
            "source_locked": True,
        }

        with patch("app.services.preload._commit_run_update", new=AsyncMock()):
            with patch("app.services.preload.build_timeline_frame_manifest", new=AsyncMock(return_value={"days": []})):
                with patch("app.services.preload._mark_manifest_cache_ready", new=AsyncMock()):
                    with patch("app.services.preload.get_timeline_context", new=AsyncMock(return_value={"status": "ok"})):
                        with patch("app.services.preload._mark_context_cache_ready", new=AsyncMock()):
                            with patch("app.services.preload.fetch_tile_png", new=AsyncMock(return_value=b"png")) as fetch_tile:
                                with patch("app.services.preload._resolve_timeline_source_metadata", new=AsyncMock(return_value=source_metadata)):
                                    with patch("app.services.preload._mark_temporal_cache_ready", new=AsyncMock()):
                                        with patch("app.services.preload.proxy_official_overlay_tile", new=AsyncMock(return_value=(b"", "image/png"))):
                                            with patch("app.services.preload._mark_overlay_cache_ready", new=AsyncMock()):
                                                with patch("app.services.preload.materialize_viewport_raster_product", new=AsyncMock()) as materialize:
                                                    await _execute_preload_run(
                                                        run_key,
                                                        run_type="viewport",
                                                        bbox=bbox,
                                                        zoom=7,
                                                        width=512,
                                                        height=512,
                                                        temporal_layers=["rgb"],
                                                        official_layers=[],
                                                        scope_type="nacional",
                                                        scope_ref="Uruguay",
                                                        timeline_scope="none",
                                                        timeline_unit_id=None,
                                                        timeline_department=None,
                                                        target_date=target,
                                                        history_days=30,
                                                        date_from=target,
                                                        date_to=target,
                                                    )

        self.assertGreaterEqual(len(materialize.await_args_list), 1)
        materialize_calls = [call.kwargs for call in materialize.await_args_list]
        self.assertTrue(
            any(
                call_kwargs.get("layer_id") == "rgb"
                and call_kwargs.get("display_date") == target
                and call_kwargs.get("bbox") == bbox
                and call_kwargs.get("zoom") == 7
                and call_kwargs.get("scope_type") == "nacional"
                and call_kwargs.get("scope_ref") == "Uruguay"
                for call_kwargs in materialize_calls
            )
        )

        self.assertGreaterEqual(len(fetch_tile.await_args_list), 1)
        fetch_kwargs = fetch_tile.await_args_list[0].kwargs
        self.assertEqual(fetch_kwargs.get("viewport_bbox"), bbox)
        self.assertEqual(fetch_kwargs.get("viewport_zoom"), 7)


class PreloadRunManagementTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        PRELOAD_TASKS.clear()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        PRELOAD_TASKS.clear()
        await engine.dispose()

    async def test_get_preload_status_marks_orphan_run_as_stale(self):
        async with AsyncSessionLocal() as session:
            row = await create_preload_run(
                session,
                run_key="orphan-run",
                run_type="viewport",
                scope_type="nacional",
                scope_ref="Uruguay",
                status="running",
                stage="official_overlays",
                details={
                    "critical_ready": True,
                    "active_stage": "critical_ready",
                    "residual_stage": "official_overlays",
                    "run_signature": "sig-1",
                },
            )
            row.updated_at = datetime.now(timezone.utc) - timedelta(seconds=settings.preload_task_liveness_seconds + 5)
            await session.commit()

        payload = await get_preload_status("orphan-run")

        self.assertEqual(payload["status"], "stale")
        self.assertEqual(payload["task_state"], "stale")
        self.assertEqual(payload["active_stage"], "critical_ready")
        self.assertEqual(payload["residual_stage"], "official_overlays")

    async def test_create_and_schedule_run_reuses_recent_matching_signature(self):
        target_date = date(2026, 4, 4)
        run_signature = _preload_run_signature(
            run_type="viewport",
            bbox="-56.0,-31.0,-55.9,-30.9",
            zoom=7,
            width=512,
            height=512,
            temporal_layers=["rgb"],
            official_layers=["coneat"],
            scope_type="nacional",
            scope_ref="Uruguay",
            timeline_scope="nacional",
            timeline_unit_id=None,
            timeline_department=None,
            date_from=target_date,
            date_to=target_date,
        )
        async with AsyncSessionLocal() as session:
            await create_preload_run(
                session,
                run_key="existing-run",
                run_type="viewport",
                scope_type="nacional",
                scope_ref="Uruguay",
                status="running",
                stage="analytic_neighbors",
                details={
                    "critical_ready": False,
                    "run_signature": run_signature,
                    "active_stage": "analytic_neighbors",
                    "residual_stage": None,
                },
            )
            await session.commit()

        with patch("app.services.preload._register_background_task", new=Mock()) as register_task:
            payload = await _create_and_schedule_run(
                run_type="viewport",
                bbox="-56.0,-31.0,-55.9,-30.9",
                zoom=7,
                width=512,
                height=512,
                temporal_layers=["rgb"],
                official_layers=["coneat"],
                scope_type="nacional",
                scope_ref="Uruguay",
                timeline_scope="nacional",
                timeline_unit_id=None,
                timeline_department=None,
                target_date=target_date,
                history_days=30,
                date_from=target_date,
                date_to=target_date,
            )

        self.assertEqual(payload["run_key"], "existing-run")
        register_task.assert_not_called()
