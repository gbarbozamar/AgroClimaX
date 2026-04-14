import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.core.config import settings
from app.main import app


class MCPIntegrationRouteTests(unittest.TestCase):
    def test_fields_search_requires_service_bearer(self):
        with patch.object(settings, "integration_service_tokens", ["svc-token"]):
            with TestClient(app) as client:
                response = client.get("/api/v1/integrations/mcp/fields/search?q=demo")
        self.assertEqual(response.status_code, 401)

    def test_fields_search_returns_payload_with_valid_bearer(self):
        payload = {
            "query": "El Trebol",
            "total": 1,
            "items": [
                {
                    "id": "farm-field-123",
                    "name": "El Trebol",
                    "aoi_unit_id": "productive-unit-abc",
                    "department": "Rivera",
                    "match_score": 1.0,
                }
            ],
        }
        with patch.object(settings, "integration_service_tokens", ["svc-token"]):
            with patch("app.api.v1.endpoints.integrations_mcp.search_fields_for_mcp", new=AsyncMock(return_value=payload)):
                with TestClient(app) as client:
                    response = client.get(
                        "/api/v1/integrations/mcp/fields/search?q=El%20Trebol&limit=3",
                        headers={"Authorization": "Bearer svc-token"},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["name"], "El Trebol")

    def test_field_current_status_contract(self):
        payload = {
            "scope_type": "field",
            "scope_id": "farm-field-123",
            "aoi_unit_id": "productive-unit-abc",
            "selection_label": "El Trebol",
            "field": {
                "id": "farm-field-123",
                "name": "El Trebol",
                "department": "Rivera",
                "aoi_unit_id": "productive-unit-abc",
            },
            "status": {
                "state": "Vigilancia",
                "risk_score": 54.1,
                "confidence_score": 69.8,
                "drivers": [{"name": "spi_30d", "score": 71.0}],
            },
        }
        with patch.object(settings, "integration_service_tokens", ["svc-token"]):
            with patch(
                "app.api.v1.endpoints.integrations_mcp.get_field_current_status_for_mcp",
                new=AsyncMock(return_value=payload),
            ):
                with TestClient(app) as client:
                    response = client.get(
                        "/api/v1/integrations/mcp/fields/farm-field-123/current-status",
                        headers={"Authorization": "Bearer svc-token"},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"]["state"], "Vigilancia")

    def test_latest_satellite_coverage_contract(self):
        payload = {
            "field_id": "farm-field-123",
            "aoi_unit_id": "productive-unit-abc",
            "selection_label": "El Trebol",
            "latest_observed_date": "2026-04-05T03:00:00+00:00",
            "source_mode": "warehouse_snapshots",
            "layers": [
                {
                    "layer_id": "rgb",
                    "visual_state": "ready",
                    "cloud_pixel_pct": 12.3,
                    "renderable_pixel_pct": 84.2,
                }
            ],
        }
        with patch.object(settings, "integration_service_tokens", ["svc-token"]):
            with patch(
                "app.api.v1.endpoints.integrations_mcp.get_latest_satellite_coverage_for_mcp",
                new=AsyncMock(return_value=payload),
            ):
                with TestClient(app) as client:
                    response = client.get(
                        "/api/v1/integrations/mcp/fields/farm-field-123/latest-satellite-coverage",
                        headers={"Authorization": "Bearer svc-token"},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["layers"][0]["visual_state"], "ready")

    def test_paddock_historical_trend_contract(self):
        payload = {
            "scope_type": "paddock",
            "scope_id": "farm-paddock-5",
            "selection_label": "Potrero 5",
            "days": 30,
            "series": {
                "ndmi": {
                    "available": True,
                    "unit": "index",
                    "points": [{"date": "2026-04-05", "value": 0.12, "source": "unit_index_snapshots"}],
                    "reason": None,
                },
                "sar_vv_db": {
                    "available": True,
                    "unit": "dB",
                    "points": [{"date": "2026-04-05", "value": -15.3, "source": "unit_index_snapshots"}],
                    "reason": None,
                },
            },
            "missing_series": ["ndvi", "ndwi", "savi"],
            "latest_observed_date": "2026-04-05",
        }
        with patch.object(settings, "integration_service_tokens", ["svc-token"]):
            with patch(
                "app.api.v1.endpoints.integrations_mcp.get_paddock_historical_trend_for_mcp",
                new=AsyncMock(return_value=payload),
            ):
                with TestClient(app) as client:
                    response = client.get(
                        "/api/v1/integrations/mcp/paddocks/farm-paddock-5/historical-trend?days=30",
                        headers={"Authorization": "Bearer svc-token"},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["series"]["ndmi"]["points"][0]["value"], 0.12)


if __name__ == "__main__":
    unittest.main()
