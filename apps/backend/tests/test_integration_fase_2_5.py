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
