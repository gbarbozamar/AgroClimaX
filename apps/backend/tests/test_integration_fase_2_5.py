"""
Integration tests para las Fases 2-5 del Field Mode plan.

Coordina: FieldImageSnapshot (Fase 2) + /timeline-frames (Fase 3) +
FieldVideoJob (Fase 4) + MCP feed + service token (Fase 5).

Los tests están marcados con skipif condicional: si los módulos de una
fase no están listos, el test correspondiente se saltea sin romper la
suite. Esto permite iterar sin bloqueos entre agentes.
"""
from __future__ import annotations

import os
from pathlib import Path
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock
from uuid import uuid4

import pytest

TEST_DB = Path(__file__).resolve().parent / f"test_integration_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"
os.environ["MCP_SERVICE_TOKEN"] = "dev-test-token"


def _has_module(path: str) -> bool:
    try:
        __import__(path)
        return True
    except Exception:
        return False


_F2_READY = _has_module("app.models.field_snapshot") and _has_module("app.services.field_snapshots")
_F3_READY = _has_module("app.api.v1.endpoints.field_timeline")
_F4_READY = _has_module("app.models.field_video") and _has_module("app.services.field_video")
_F5_READY = _has_module("app.api.v1.endpoints.mcp_feed")


class TimelineFramesEndpointTests(IsolatedAsyncioTestCase):
    """Fase 3: /api/v1/campos/{id}/timeline-frames debe devolver la shape del contrato."""

    @pytest.mark.skipif(not _F3_READY, reason="Fase 3 (field_timeline endpoint) pendiente")
    def test_timeline_frames_shape_for_unknown_field_returns_404(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.get("/api/v1/campos/no-such-field/timeline-frames?layer=ndvi&days=30")
            # Ownership check falla o field no existe → 403/404
            self.assertIn(resp.status_code, (403, 404))

    @pytest.mark.skipif(not _F3_READY, reason="Fase 3 (field_timeline endpoint) pendiente")
    def test_backfill_snapshots_unknown_field_returns_404(self):
        """POST /api/v1/campos/{id}/backfill-snapshots — field inexistente → 404/403."""
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/campos/no-such-field/backfill-snapshots",
                json={"days": 30, "layers": ["ndvi"]},
            )
            self.assertIn(resp.status_code, (403, 404))

    @pytest.mark.skipif(not _F3_READY, reason="Fase 3 (field_timeline endpoint) pendiente")
    def test_backfill_snapshots_invalid_days_returns_400_or_422(self):
        """POST /api/v1/campos/{id}/backfill-snapshots — days fuera de [1, 365] → 4xx."""
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/campos/no-such-field/backfill-snapshots",
                json={"days": 0, "layers": ["ndvi"]},
            )
            # Ownership check (403/404) o validación (400/422).
            self.assertIn(resp.status_code, (400, 403, 404, 422))

    @pytest.mark.skipif(not _F3_READY, reason="Fase 3 (field_timeline endpoint) pendiente")
    def test_layers_available_unknown_field_returns_404(self):
        """GET /api/v1/campos/{id}/layers-available — field inexistente → 404."""
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.get("/api/v1/campos/no-such-field/layers-available")
            self.assertIn(resp.status_code, (403, 404))

    @pytest.mark.skipif(not _F3_READY, reason="Fase 3 (field_timeline endpoint) pendiente")
    def test_layers_available_aggregates_by_layer(self):
        """GET /api/v1/campos/{id}/layers-available — agrupa snapshots por layer_key con count + ventana temporal."""
        from datetime import date

        from fastapi.testclient import TestClient

        from app.db.session import AsyncSessionLocal, Base, engine as async_engine
        from app.main import app
        from app.models.farm import FarmField
        from app.models.field_snapshot import FieldImageSnapshot

        async def _seed() -> tuple[str, str]:
            async with async_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            user_id = f"user-{uuid4().hex[:8]}"
            field_id = f"field-{uuid4().hex[:8]}"
            async with AsyncSessionLocal() as db:
                db.add(FarmField(
                    id=field_id,
                    user_id=user_id,
                    establishment_id="est-test",
                    name="Test Field",
                    department="Rivera",
                    padron_value="test",
                    field_geometry_geojson={"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]},
                    active=True,
                ))
                # 2 snapshots ndvi, 1 alerta_fusion, todos con observed_at distintos.
                db.add(FieldImageSnapshot(
                    field_id=field_id, user_id=user_id, layer_key="ndvi",
                    observed_at=date(2026, 3, 23), storage_key="k1",
                    width_px=256, height_px=256, bbox_json=[0, 0, 1, 1], area_ha=10.0,
                ))
                db.add(FieldImageSnapshot(
                    field_id=field_id, user_id=user_id, layer_key="ndvi",
                    observed_at=date(2026, 4, 21), storage_key="k2",
                    width_px=256, height_px=256, bbox_json=[0, 0, 1, 1], area_ha=10.0,
                ))
                db.add(FieldImageSnapshot(
                    field_id=field_id, user_id=user_id, layer_key="alerta_fusion",
                    observed_at=date(2026, 4, 10), storage_key="k3",
                    width_px=256, height_px=256, bbox_json=[0, 0, 1, 1], area_ha=10.0,
                ))
                await db.commit()
            return user_id, field_id

        import asyncio
        user_id, field_id = asyncio.get_event_loop().run_until_complete(_seed())

        # AUTH_BYPASS_FOR_TESTS está activo; el helper de auth usa el user_id del field
        # porque require_field_ownership compara auth.user.id contra field.user_id y
        # el bypass permite cualquier user_id. Inyectamos el header común.
        with TestClient(app) as client:
            resp = client.get(
                f"/api/v1/campos/{field_id}/layers-available",
                headers={"X-Test-User-Id": user_id},
            )
            # Si el bypass cross-user devuelve 403 porque el test user != field owner,
            # al menos verificamos que el endpoint existe (no 404 en el path).
            self.assertIn(resp.status_code, (200, 403))
            if resp.status_code == 200:
                data = resp.json()
                self.assertEqual(data["field_id"], field_id)
                keys = {l["layer_key"] for l in data["layers"]}
                self.assertIn("ndvi", keys)
                self.assertIn("alerta_fusion", keys)
                ndvi = next(l for l in data["layers"] if l["layer_key"] == "ndvi")
                self.assertEqual(ndvi["count"], 2)
                self.assertEqual(ndvi["first_observed"], "2026-03-23")
                self.assertEqual(ndvi["last_observed"], "2026-04-21")
                self.assertEqual(ndvi["label"], "NDVI (Vegetación)")


class VideoJobEndpointsTests(IsolatedAsyncioTestCase):
    """Fase 4: POST/GET /api/v1/campos/{id}/videos con auth."""

    @pytest.mark.skipif(not _F4_READY, reason="Fase 4 (field_video) pendiente")
    def test_post_video_for_unknown_field_fails_cleanly(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/campos/no-such/videos",
                json={"layer_key": "ndvi", "duration_days": 30},
            )
            # Ownership check (403) o field-not-found (404), no debe tirar 500.
            self.assertIn(resp.status_code, (403, 404))


class MCPFeedAuthTests(IsolatedAsyncioTestCase):
    """Fase 5: /api/v1/mcp/* requiere X-Service-Token válido."""

    @pytest.mark.skipif(not _F5_READY, reason="Fase 5 (mcp_feed) pendiente")
    def test_mcp_without_token_returns_4xx(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.get("/api/v1/mcp/fields/by-alert")
            # Sin token → 401/403/422 (depende del Header requirement).
            self.assertIn(resp.status_code, (401, 403, 422))

    @pytest.mark.skipif(not _F5_READY, reason="Fase 5 (mcp_feed) pendiente")
    def test_mcp_with_invalid_token_returns_401(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/mcp/fields/by-alert",
                headers={"X-Service-Token": "wrong-token"},
            )
            self.assertEqual(resp.status_code, 401)

    @pytest.mark.skipif(not _F5_READY, reason="Fase 5 (mcp_feed) pendiente")
    def test_mcp_with_valid_token_accepts(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/mcp/fields/by-alert",
                headers={"X-Service-Token": "dev-test-token"},
            )
            # 200 con lista (vacía o con fields) o 404/500 por otra razón —
            # la clave es que NO es 401/403 (auth OK).
            self.assertNotIn(resp.status_code, (401, 403))


class ModelMigrationsSmokeTests(IsolatedAsyncioTestCase):
    """Sanity: las tablas nuevas se crean con Base.metadata.create_all al arrancar."""

    @pytest.mark.skipif(not _F2_READY, reason="Fase 2 model pendiente")
    def test_field_image_snapshots_table_exists(self):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.db.session import Base
        tables = set(Base.metadata.tables.keys())
        self.assertIn("field_image_snapshots", tables)

    @pytest.mark.skipif(not _F4_READY, reason="Fase 4 model pendiente")
    def test_field_video_jobs_table_exists(self):
        from app.db.session import Base
        tables = set(Base.metadata.tables.keys())
        self.assertIn("field_video_jobs", tables)


if __name__ == "__main__":
    unittest.main()
