"""Tests para que generate_field_video use SOLO frames reales disponibles."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from uuid import uuid4

TEST_DB = Path(__file__).resolve().parent / f"test_video_real_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"

from app.db.session import AsyncSessionLocal, Base, engine as async_engine
import app.models  # noqa: F401 - register all ORM models on Base.metadata


async def _reset_schema():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


class VideoFrameCountTests(IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await _reset_schema()

    async def test_insufficient_frames_marks_job_failed(self):
        """Si hay < 2 snapshots reales, job queda failed con error_message claro."""
        from app.models.field_video import FieldVideoJob
        from app.services.field_video import generate_field_video
        from sqlalchemy import select

        async with AsyncSessionLocal() as db:
            job = FieldVideoJob(
                field_id=f"f-{uuid4().hex[:8]}",
                user_id=f"u-{uuid4().hex[:8]}",
                layer_key="ndvi",
                duration_days=30,
                status="queued",
                progress_pct=0.0,
            )
            db.add(job)
            await db.commit()
            await db.refresh(job)

            result = await generate_field_video(db, job.id)
            self.assertEqual(result["status"], "failed")
            await db.refresh(job)
            self.assertEqual(job.status, "failed")
            self.assertIn("insufficient_frames", job.error_message or "")

    async def test_video_response_includes_requested_and_covered_days(self):
        """El endpoint _serialize debe incluir requested_days y covered_days."""
        from app.api.v1.endpoints.field_video import _serialize
        from app.models.field_video import FieldVideoJob

        job = FieldVideoJob(
            id=str(uuid4()),
            field_id="f-x", user_id="u-x",
            layer_key="ndvi", duration_days=30,
            status="ready", progress_pct=100.0,
            error_message="covers 12 real days out of 30 requested",
        )
        d = _serialize(job)
        self.assertEqual(d["requested_days"], 30)
        self.assertEqual(d["covered_days"], 12)
        self.assertEqual(d["status"], "ready")


class SnapshotValidationTests(IsolatedAsyncioTestCase):
    """Test que render_field_snapshot no crea rows cuando PNG es placeholder."""

    async def asyncSetUp(self):
        await _reset_schema()

    async def test_returns_none_when_all_tiles_are_placeholders(self):
        """Con fetch_tile_png devolviendo placeholders (<500b), result=None y no row en DB."""
        from app.services.field_snapshots import render_field_snapshot

        async def _fake_fetch(*args, **kwargs):
            return b"\x89PNG\r\n\x1a\n" + b"\x00" * 100  # 108b, placeholder

        from shapely.geometry import Polygon
        poly = Polygon([(-55.54,-31.08),(-55.53,-31.08),(-55.53,-31.07),(-55.54,-31.07)])

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return poly

        with patch("app.services.field_snapshots.fetch_tile_png", side_effect=_fake_fetch), \
             patch("app.services.field_snapshots.resolve_scope_geometry", side_effect=_fake_resolve):
            async with AsyncSessionLocal() as db:
                r = await render_field_snapshot(db, "f-test", "ndvi", date(2026, 4, 21))
                self.assertIsNone(r)


if __name__ == "__main__":
    unittest.main()
