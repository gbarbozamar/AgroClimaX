import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app


class ApiContractTests(unittest.TestCase):
    def test_v1_estado_actual_contract(self):
        payload = {
            "scope": "departamento",
            "unit_id": "department-rivera",
            "unit_name": "Rivera",
            "department": "Rivera",
            "observed_at": "2026-03-23T12:00:00+00:00",
            "state": "Alerta",
            "state_level": 2,
            "legacy_level": "NARANJA",
            "color": "#e67e22",
            "risk_score": 68.4,
            "confidence_score": 74.2,
            "affected_pct": 42.0,
            "largest_cluster_pct": 19.5,
            "days_in_state": 4,
            "actionable": True,
            "drivers": [],
            "forecast": [],
            "soil_context": {},
            "calibration_ref": "cal-1",
            "data_mode": "simulated",
            "explanation": "demo",
            "raw_metrics": {},
        }
        with patch("app.api.v1.endpoints.alertas.get_scope_snapshot", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/alertas/estado-actual?scope=departamento&department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["risk_score"], 68.4)

    def test_legacy_estado_actual_contract(self):
        payload = {
            "fecha": "2026-03-23",
            "departamento": "Rivera",
            "alerta": {"nivel": "AMARILLO", "codigo": 1, "color": "#f1c40f"},
            "sentinel_1": {"humedad_media": 44.0},
            "sentinel_2": {"ndmi_media": 0.03},
            "era5": {"spi_30d": -0.7, "spi_categoria": "normal"},
            "resumen": {"humedad_s1_pct": 44.0, "ndmi_s2": 0.03},
            "dias_deficit": 2,
            "es_prolongada": False,
        }
        with patch("app.api.v1.endpoints.legacy.get_legacy_state", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/estado-actual?department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["alerta"]["nivel"], "AMARILLO")


if __name__ == "__main__":
    unittest.main()
