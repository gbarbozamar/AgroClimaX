import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from fastapi.testclient import TestClient

from app.main import app


class ApiContractTests(unittest.TestCase):
    def test_v1_capas_departamentos_contract(self):
        payload = {
            "type": "FeatureCollection",
            "metadata": {"count": 1, "scope": "departamentos", "cache_status": "current"},
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-56.4, -31.2], [-55.7, -31.2], [-55.7, -31.8], [-56.4, -31.8], [-56.4, -31.2]]],
                    },
                    "properties": {"unit_id": "department-rivera", "department": "Rivera", "risk_score": 41.2},
                }
            ],
        }
        with patch("app.api.v1.endpoints.layers.get_cached_layer_features", new=AsyncMock(return_value=[object()])):
            with patch("app.api.v1.endpoints.layers.build_feature_collection", return_value=payload):
                with TestClient(app) as client:
                    response = client.get("/api/v1/capas/departamentos")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["metadata"]["scope"], "departamentos")

    def test_v1_capas_secciones_contract(self):
        payload = {
            "type": "FeatureCollection",
            "metadata": {"count": 1, "scope": "secciones_policiales"},
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-56.0, -31.0], [-55.9, -31.0], [-55.9, -31.1], [-56.0, -31.1], [-56.0, -31.0]]],
                    },
                    "properties": {"unit_id": "section-police-1301", "department": "Rivera", "risk_score": 41.2},
                }
            ],
        }
        with patch("app.api.v1.endpoints.layers.materialize_police_section_cache", new=AsyncMock(return_value={"count": 1})):
            with patch("app.api.v1.endpoints.layers.get_cached_layer_features", new=AsyncMock(return_value=[])):
                with patch("app.api.v1.endpoints.layers.build_feature_collection", return_value=payload):
                    with TestClient(app) as client:
                        response = client.get("/api/v1/capas/secciones?department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["metadata"]["scope"], "secciones_policiales")

    def test_v1_capas_hexagonos_contract(self):
        payload = {
            "type": "FeatureCollection",
            "metadata": {"count": 1, "scope": "hexagonos_h3", "resolution": 6},
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-56.0, -31.0], [-55.95, -31.0], [-55.93, -31.04], [-55.97, -31.08], [-56.02, -31.06], [-56.03, -31.02], [-56.0, -31.0]]],
                    },
                    "properties": {"unit_id": "h3-r6-86abc", "department": "Rivera", "risk_score": 41.2, "h3_resolution": 6},
                }
            ],
        }
        with patch("app.api.v1.endpoints.layers.get_cached_layer_features", new=AsyncMock(return_value=[object()])):
            with patch("app.api.v1.endpoints.layers.build_feature_collection", return_value=payload):
                with TestClient(app) as client:
                    response = client.get("/api/v1/capas/hexagonos?department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["metadata"]["scope"], "hexagonos_h3")

    def test_v1_capas_productivas_contract(self):
        payload = {
            "type": "FeatureCollection",
            "metadata": {"count": 1, "scope": "unidades_productivas"},
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-56.0, -31.0], [-55.94, -31.0], [-55.94, -31.06], [-56.0, -31.06], [-56.0, -31.0]]],
                    },
                    "properties": {"unit_id": "productive-predio-demo", "department": "Rivera", "unit_category": "predio", "risk_score": 41.2},
                }
            ],
        }
        with patch("app.api.v1.endpoints.layers.get_cached_layer_features", new=AsyncMock(return_value=[object()])):
            with patch("app.api.v1.endpoints.layers.build_feature_collection", return_value=payload):
                with TestClient(app) as client:
                    response = client.get("/api/v1/capas/productivas?department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["metadata"]["scope"], "unidades_productivas")

    def test_v1_secciones_geojson_contract(self):
        payload = {
            "type": "FeatureCollection",
            "metadata": {"count": 1, "scope": "secciones_policiales"},
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-56.0, -31.0], [-55.9, -31.0], [-55.9, -31.1], [-56.0, -31.1], [-56.0, -31.0]]],
                    },
                    "properties": {
                        "unit_id": "section-police-1301",
                        "unit_name": "Seccion Policial SP 1 - Rivera",
                        "department": "Rivera",
                        "state": "Vigilancia",
                        "risk_score": 41.2,
                    },
                },
            ],
        }
        with patch("app.api.v1.endpoints.sections.police_sections_geojson", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/secciones/geojson?department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["metadata"]["count"], 1)
        self.assertEqual(response.json()["features"][0]["properties"]["department"], "Rivera")

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

    def test_v1_pipeline_estado_contract(self):
        payload = {
            "target_date": "2026-03-24",
            "units": 19,
            "live_units": 15,
            "carry_forward_units": 2,
            "simulated_units": 2,
            "ready": True,
            "needs_live_refresh": False,
            "scheduler": {
                "enabled": True,
                "timezone": "America/Montevideo",
                "poll_seconds": 300,
                "bootstrap_backfill_days": 7,
                "next_daily_run": "2026-03-25T06:30:00+00:00",
                "next_recalibration_run": "2026-03-30T06:30:00+00:00",
            },
            "runs": {"last_daily_success": None, "last_recalibration_success": None, "recent": []},
            "pending_backfill_dates": [],
        }
        with patch("app.api.v1.endpoints.pipeline.get_pipeline_status", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/pipeline/estado")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["scheduler"]["enabled"])

    def test_v1_productivas_import_contract(self):
        payload = {
            "status": "success",
            "category": "predio",
            "source_name": "demo",
            "features_received": 1,
            "created": 1,
            "updated": 0,
            "skipped": 0,
            "unit_ids": ["productive-predio-demo"],
        }
        body = {
            "feature_collection": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[-56.0, -31.0], [-55.94, -31.0], [-55.94, -31.06], [-56.0, -31.06], [-56.0, -31.0]]],
                        },
                        "properties": {"name": "Predio Demo"},
                    }
                ],
            },
            "category": "predio",
            "source_name": "demo",
        }
        with patch("app.api.v1.endpoints.productivas.import_productive_units", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/productivas/import", json=body)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["created"], 1)

    def test_v1_productivas_import_archivo_contract(self):
        payload = {
            "status": "success",
            "category": "potrero",
            "source_name": "upload_demo",
            "features_received": 1,
            "created": 1,
            "updated": 0,
            "skipped": 0,
            "unit_ids": ["productive-potrero-demo"],
            "filename": "potreros.zip",
            "file_format": "zip_shapefile",
        }
        with patch("app.api.v1.endpoints.productivas.import_productive_units_file", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/productivas/import-archivo",
                    files={"file": ("potreros.zip", b"fake-binary", "application/zip")},
                    data={"category": "potrero", "source_name": "upload_demo"},
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["file_format"], "zip_shapefile")

    def test_v1_productivas_plantilla_contract(self):
        with TestClient(app) as client:
            response = client.get("/api/v1/productivas/plantilla")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["type"], "FeatureCollection")
        self.assertEqual(len(response.json()["features"]), 1)

    def test_v1_pipeline_backfill_contract(self):
        payload = {
            "status": "success",
            "start_date": "2026-03-20",
            "end_date": "2026-03-24",
            "processed_days": 5,
            "include_recalibration": True,
            "runs": [{"date": "2026-03-24", "daily": "success", "recalibration": "success"}],
        }
        with patch("app.api.v1.endpoints.pipeline.run_historical_backfill", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/pipeline/backfill?fecha_desde=2026-03-20&fecha_hasta=2026-03-24")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["processed_days"], 5)

    def test_v1_pipeline_prewarm_coneat_contract(self):
        payload = {
            "status": "success",
            "result": {
                "status": "success",
                "planned_tiles": 18,
                "reused_tiles": 12,
                "warmed_tiles": 6,
                "cache_backend": "database+filesystem",
            },
            "job": {"job_type": "coneat_prewarm", "status": "success"},
        }
        with patch("app.api.v1.endpoints.pipeline.execute_coneat_prewarm_job", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/pipeline/prewarm-coneat?department=Rivera")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["result"]["cache_backend"], "database+filesystem")

    def test_v1_notificaciones_suscriptores_contract(self):
        payload = [
            {
                "id": "sub-1",
                "nombre": "Tecnico Demo",
                "email": "demo@example.com",
                "telefono": None,
                "whatsapp": "+59899111222",
                "departamento": "Rivera",
                "unit_id": "productive-predio-demo",
                "nivel_minimo": 2,
                "activo": True,
                "metadata_extra": {},
            }
        ]
        with patch("app.api.v1.endpoints.notifications.notification_service.list_subscribers", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/notificaciones/suscriptores?unit_id=productive-predio-demo")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total"], 1)
        self.assertEqual(response.json()["datos"][0]["nombre"], "Tecnico Demo")

    def test_v1_notificaciones_eventos_contract(self):
        payload = [
            {
                "id": "notif-1",
                "alert_event_id": "alert-1",
                "channel": "dashboard",
                "recipient": "dashboard:sub-1:productive-predio-demo",
                "status": "stored",
                "reason": "state_change",
                "title": "AgroClimaX | Alerta | Predio Demo",
                "body": "Predio Demo entra en alerta.",
                "unit_id": "productive-predio-demo",
                "department": "Rivera",
                "state": "Alerta",
            }
        ]
        with patch("app.api.v1.endpoints.notifications.notification_service.list_notification_events", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/notificaciones/eventos?unit_id=productive-predio-demo")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total"], 1)
        self.assertEqual(response.json()["datos"][0]["channel"], "dashboard")

    def test_v1_settings_schema_contract(self):
        with TestClient(app) as client:
            response = client.get("/api/v1/settings/schema")
        self.assertEqual(response.status_code, 200)
        self.assertIn("schema", response.json())
        self.assertIn("defaults", response.json())

    def test_v1_settings_payload_contract(self):
        payload = {
            "global": {"risk_weights": {"magnitude": 35.0}},
            "global_version": 2,
            "global_updated_at": "2026-03-27T00:00:00+00:00",
            "global_updated_by_label": "ops",
            "overrides": {"forestal": {"rules": {"risk_weights": {"weather": 20.0}}, "version": 1}},
            "effective_by_coverage": {"forestal": {"risk_weights": {"weather": 20.0}}},
            "coverage_labels": {"forestal": "Forestal"},
            "coverage_classes": [{"key": "forestal", "label": "Forestal"}],
            "recalculation_status": {"status": "completed", "window_days": 30},
            "rules_version": "global-v2::forestal-v1",
        }
        with patch("app.api.v1.endpoints.settings.get_settings_payload", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/settings?coverage_class=forestal")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["rules_version"], "global-v2::forestal-v1")

    def test_v1_settings_save_contract(self):
        payload = {
            "status": "success",
            "scope_type": "global",
            "scope_key": "global",
            "rules_version": "global-v3",
            "recalculation_status": {"status": "completed", "window_days": 30},
            "settings": {"global_version": 3},
        }
        with patch("app.api.v1.endpoints.settings.save_global_settings", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.put(
                    "/api/v1/settings/global",
                    json={"rules": {"risk_weights": {"magnitude": 40.0}}, "operator_label": "tester"},
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["scope_type"], "global")

    def test_v1_profile_schema_contract(self):
        payload = {
            "questionnaire_version": "v1",
            "required_fields": ["role_code", "organization_type"],
            "catalogs": {
                "organization_types": [{"value": "productor", "label": "Productor"}],
                "role_codes": [{"value": "productor", "label": "Productor"}],
                "scope_types": [{"value": "nacional", "label": "Nacional"}],
                "production_types": [{"value": "ganaderia", "label": "Ganaderia"}],
                "use_cases": [{"value": "monitoreo_diario", "label": "Monitoreo diario"}],
                "alert_channels": [{"value": "email", "label": "Email"}],
                "min_alert_states": [{"value": "Alerta", "label": "Alerta"}],
                "preferred_languages": [{"value": "es-UY", "label": "Espanol (Uruguay)"}],
                "departments": [{"id": "department-rivera", "label": "Rivera"}],
                "jurisdictions": [{"id": "section-police-1301", "label": "Seccion Policial SP 1 - Rivera", "department": "Rivera"}],
            },
        }
        with patch("app.api.v1.endpoints.profile.get_profile_schema", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/profile/schema")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["questionnaire_version"], "v1")

    def test_v1_profile_me_contract(self):
        payload = {
            "google_identity": {
                "id": "user-1",
                "google_sub": "google-123",
                "email": "demo@example.com",
                "email_verified": True,
                "full_name": "Usuario Demo",
                "given_name": "Usuario",
                "family_name": "Demo",
                "picture_url": None,
                "locale": "es-UY",
                "is_active": True,
                "last_login_at": "2026-03-29T00:00:00+00:00",
                "created_at": "2026-03-28T00:00:00+00:00",
            },
            "profile": {
                "organization_type": "productor",
                "scope_type": "nacional",
                "scope_ids_json": [],
                "use_cases_json": ["monitoreo_diario"],
                "alert_channels_json": ["email"],
                "completion_pct": 100.0,
            },
            "completion": {
                "is_complete": True,
                "completion_pct": 100.0,
                "questionnaire_version": "v1",
                "completed_at": "2026-03-29T00:10:00+00:00",
                "missing_fields": [],
            },
            "options": {
                "organization_types": [{"value": "productor", "label": "Productor"}],
            },
        }
        with patch("app.api.v1.endpoints.profile.get_profile_me", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/profile/me")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["completion"]["is_complete"])

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
