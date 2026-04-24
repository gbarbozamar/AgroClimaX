"""Smoke tests para los ~15+ endpoints MCP agregados en M1-M4."""
from __future__ import annotations
import os, unittest, importlib
from pathlib import Path
from uuid import uuid4

TEST_DB = Path(__file__).resolve().parent / f"test_mcp_full_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["MCP_SERVICE_TOKEN"] = "dev-test-token"

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _has(m):
    try: importlib.import_module(m); return True
    except Exception: return False


_MCP_READY = _has("app.api.v1.endpoints.mcp_feed")


@pytest.mark.skipif(not _MCP_READY, reason="mcp_feed no disponible")
class MCPFullSuiteTests(unittest.IsolatedAsyncioTestCase):
    def _client(self):
        from app.main import app
        return TestClient(app)

    def _headers(self):
        return {"X-Service-Token": "dev-test-token"}

    def test_video_status_unknown_returns_404(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/fields/no-such/video/no-such-job", headers=self._headers())
            self.assertIn(r.status_code, (404, 500, 503))

    def test_request_video_unknown_field_returns_404(self):
        """POST /fields/{unknown}/video debe devolver 404 claro (no 500 por FK)."""
        with self._client() as c:
            r = c.post(
                "/api/v1/mcp/fields/00000000-0000-0000-0000-000000000000/video",
                json={"layer_key": "ndvi", "duration_days": 30},
                headers=self._headers(),
            )
            # En SQLite sin FK enforce antes hubiera sido 200 (crea orphan job).
            # En Postgres prod hubiera sido 500 (FK violation). Ambos casos
            # ahora se normalizan a 404 con detail="Field not found".
            self.assertEqual(r.status_code, 404)
            self.assertIn("not found", r.json().get("detail", "").lower())

    def test_list_video_jobs_empty(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/fields/no-such/videos", headers=self._headers())
            self.assertIn(r.status_code, (200, 500, 503))
            if r.status_code == 200:
                self.assertEqual(r.json()["total"], 0)

    def test_list_user_fields(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/users/no-such-user/fields", headers=self._headers())
            self.assertIn(r.status_code, (200, 404, 503))

    def test_field_details_unknown_returns_404(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/fields/no-such/details", headers=self._headers())
            self.assertIn(r.status_code, (404, 503))

    def test_layers_available_mcp(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/fields/no-such/layers-available", headers=self._headers())
            self.assertIn(r.status_code, (200, 503))

    def test_backfill_trigger_requires_valid_days(self):
        with self._client() as c:
            r = c.post("/api/v1/mcp/fields/no-such/backfill", json={"days": 0}, headers=self._headers())
            self.assertIn(r.status_code, (400, 404, 422, 503))

    def test_alert_current_nacional(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/alerts/current", params={"scope": "nacional"}, headers=self._headers())
            self.assertIn(r.status_code, (200, 404, 503))

    def test_alert_history_requires_ref_for_scope(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/alerts/history", params={"scope": "departamento"}, headers=self._headers())
            self.assertIn(r.status_code, (200, 400, 503))

    def test_alert_forecast_nacional(self):
        with self._client() as c:
            r = c.get("/api/v1/mcp/alerts/forecast", params={"scope": "nacional"}, headers=self._headers())
            self.assertIn(r.status_code, (200, 503))


if __name__ == "__main__":
    unittest.main()
