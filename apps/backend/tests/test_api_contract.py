import os
from datetime import date
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
    def test_v1_map_overlay_catalog_contract(self):
        payload = {
            "items": [
                {
                    "id": "coneat",
                    "label": "CONEAT",
                    "category": "Suelos",
                    "provider": "SNIG / MGAP",
                    "service_kind": "arcgis_export",
                    "service_url": "https://example.invalid/coneat/export",
                    "layers": "show:0,1",
                    "min_zoom": 11,
                    "opacity_default": 0.96,
                    "z_index_priority": 330,
                    "attribution": "SNIG / MGAP Uruguay",
                    "cache_namespace": "renare_export_v1",
                    "recommended": True,
                }
            ]
        }
        with patch("app.api.v1.endpoints.public.list_official_map_overlays", return_value=payload["items"]):
            with TestClient(app) as client:
                response = client.get("/api/v1/map-overlays/catalog")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["id"], "coneat")

    def test_v1_map_overlay_tile_contract(self):
        with patch(
            "app.api.v1.endpoints.public.proxy_official_overlay_tile",
            new=AsyncMock(return_value=(b"png-bytes", "image/png")),
        ):
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/map-overlays/hidrografia/tile"
                    "?bbox=-58,-35,-53,-30&bboxSR=4326&imageSR=4326&width=256&height=256&format=image/png&transparent=true"
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/png")
        self.assertEqual(response.content, b"png-bytes")

    def test_v1_timeline_frames_contract(self):
        payload = {
            "date_from": "2026-03-30",
            "date_to": "2026-04-03",
            "total_days": 5,
            "generated_at": "2026-04-03T00:00:00",
            "bbox": "-56,-32,-55,-31",
            "zoom": 11,
            "layers": ["ndmi", "rgb"],
            "days": [
                {
                    "display_date": "2026-04-03",
                    "available": True,
                    "label": "2026-04-03 · Interpolado",
                    "layers": {
                        "ndmi": {
                            "layer_id": "ndmi",
                            "available": True,
                            "is_interpolated": True,
                            "primary_source_date": "2026-04-01",
                            "secondary_source_date": "2026-04-06",
                            "blend_weight": 0.4,
                            "label": "Interpolado",
                            "cache_status": "ready",
                            "warm_available": True,
                        }
                    },
                }
            ],
        }
        with patch("app.api.v1.endpoints.public.build_timeline_frame_manifest", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/timeline/frames?layers=ndmi&layers=rgb&date_from=2026-03-30&date_to=2026-04-03"
                    "&bbox=-56,-32,-55,-31&zoom=11"
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total_days"], 5)
        self.assertEqual(response.json()["days"][0]["layers"]["ndmi"]["primary_source_date"], "2026-04-01")
        self.assertTrue(response.json()["days"][0]["layers"]["ndmi"]["warm_available"])

    def test_v1_timeline_context_contract(self):
        payload = {
            "scope": "departamento",
            "unit_id": "department-rivera",
            "department": "Rivera",
            "selection_label": "Rivera",
            "display_date": "2026-03-31",
            "resolved_date": "2026-03-30",
            "is_interpolated": True,
            "forecast_mode": "collapsed_historical",
            "weather_mode": "collapsed_historical",
            "cache_status": "fallback_previous",
            "state_payload": {
                "scope": "departamento",
                "unit_id": "department-rivera",
                "department": "Rivera",
                "observed_at": "2026-03-30T03:00:00+00:00",
                "state": "Alerta",
                "state_level": 2,
                "risk_score": 64.2,
                "confidence_score": 71.4,
                "affected_pct": 23.5,
                "largest_cluster_pct": 18.2,
                "days_in_state": 4,
                "drivers": [{"name": "spi_30d", "score": 71.0}],
                "forecast": [],
                "soil_context": {},
                "raw_metrics": {"spi_30d": -1.7},
            },
            "history_payload": {
                "scope": "departamento",
                "unit_id": "department-rivera",
                "department": "Rivera",
                "selection_label": "Rivera",
                "total": 2,
                "datos": [
                    {"fecha": "2026-03-30", "state": "Alerta", "state_level": 2, "risk_score": 64.2, "confidence_score": 71.4, "affected_pct": 23.5, "largest_cluster_pct": 18.2, "drivers": []},
                    {"fecha": "2026-03-29", "state": "Vigilancia", "state_level": 1, "risk_score": 54.1, "confidence_score": 69.8, "affected_pct": 19.2, "largest_cluster_pct": 15.1, "drivers": []},
                ],
            },
        }
        with patch("app.api.v1.endpoints.public.get_timeline_context", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/timeline/context"
                    "?scope=departamento&department=Rivera&target_date=2026-03-31&history_days=30"
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["display_date"], "2026-03-31")
        self.assertEqual(response.json()["history_payload"]["datos"][0]["fecha"], "2026-03-30")

    def test_v1_temporal_tile_contract_accepts_source_date(self):
        with patch(
            "app.api.v1.endpoints.public.fetch_tile_png",
            new=AsyncMock(return_value=b"timeline-png"),
        ) as fetch_tile:
            with TestClient(app) as client:
                response = client.get("/api/v1/tiles/ndmi/7/45/63.png?source_date=2026-04-01&frame_role=primary")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/png")
        self.assertEqual(response.content, b"timeline-png")
        # Test laxo: solo verificar args principales; los kwargs clip_scope/clip_ref/db se agregaron
        # como opcionales con defaults None y no hay que asertarlos.
        fetch_tile.assert_awaited_once()
        call_args = fetch_tile.await_args
        self.assertEqual(call_args.args, ("ndmi", 7, 45, 63))
        self.assertEqual(call_args.kwargs.get("target_date"), date(2026, 4, 1))
        self.assertEqual(call_args.kwargs.get("frame_role"), "primary")
        self.assertIsNone(call_args.kwargs.get("clip_scope"))
        self.assertIsNone(call_args.kwargs.get("clip_ref"))

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
                "timeline_historical_window_days": 365,
                "next_daily_run": "2026-03-25T06:30:00+00:00",
                "next_recalibration_run": "2026-03-30T06:30:00+00:00",
            },
            "runs": {"last_daily_success": None, "last_recalibration_success": None, "recent": []},
            "pending_backfill_dates": [],
            "historical_warehouse": {
                "window_days": 365,
                "date_from": "2025-03-25",
                "date_to": "2026-03-24",
                "ready": False,
                "overall_coverage_pct": 72.4,
                "national": {
                    "available_days": 240,
                    "expected_days": 365,
                    "coverage_pct": 65.8,
                    "status": "partial",
                    "latest_observed_date": "2026-03-24",
                    "missing_sample": ["2025-03-25"],
                },
                "departments": {
                    "expected_departments": 19,
                    "fully_covered_departments": 11,
                    "available_day_slots": 5400,
                    "expected_day_slots": 6935,
                    "coverage_pct": 77.9,
                    "items": [],
                },
                "temporal_layers": {
                    "expected_layers": 8,
                    "fully_covered_layers": 5,
                    "items": [],
                },
            },
        }
        with patch("app.api.v1.endpoints.pipeline.get_pipeline_status", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/pipeline/estado")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["scheduler"]["enabled"])
        self.assertIn("historical_warehouse", response.json())

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
            "window_days": 5,
            "processed_days": 5,
            "include_recalibration": True,
            "runs": [{"date": "2026-03-24", "daily": "success", "recalibration": "success"}],
        }
        with patch("app.api.v1.endpoints.pipeline.run_historical_backfill", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/pipeline/backfill?fecha_desde=2026-03-20&fecha_hasta=2026-03-24")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["processed_days"], 5)

    def test_v1_pipeline_backfill_timeline_contract(self):
        payload = {
            "status": "success",
            "job": {"job_type": "timeline_backfill", "status": "success"},
            "result": {
                "status": "success",
                "start_date": "2025-04-04",
                "end_date": "2026-04-03",
                "window_days": 365,
                "processed_days": 365,
                "include_recalibration": True,
                "runs": [],
            },
            "warehouse": {
                "window_days": 365,
                "date_from": "2025-04-04",
                "date_to": "2026-04-03",
                "ready": True,
                "overall_coverage_pct": 100.0,
                "national": {"available_days": 365, "expected_days": 365, "coverage_pct": 100.0, "status": "complete", "latest_observed_date": "2026-04-03", "missing_sample": []},
                "departments": {"expected_departments": 19, "fully_covered_departments": 19, "available_day_slots": 6935, "expected_day_slots": 6935, "coverage_pct": 100.0, "items": []},
                "temporal_layers": {"expected_layers": 8, "fully_covered_layers": 8, "items": []},
            },
        }
        with patch("app.api.v1.endpoints.pipeline.execute_timeline_backfill_job", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/pipeline/backfill-timeline?window_days=365&fecha_hasta=2026-04-03")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job"]["job_type"], "timeline_backfill")
        self.assertTrue(response.json()["warehouse"]["ready"])

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

    def test_v1_preload_startup_contract(self):
        payload = {
            "run_key": "preload-1",
            "run_type": "startup",
            "scope_type": "nacional",
            "scope_ref": "Uruguay",
            "status": "queued",
            "progress_total": 12,
            "progress_done": 0,
            "stage": "queued",
            "details": {"critical_ready": False},
        }
        with patch("app.api.v1.endpoints.public.start_startup_preload", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/preload/startup",
                    json={
                        "bbox": "-56,-32,-55,-31",
                        "zoom": 11,
                        "width": 1200,
                        "height": 700,
                        "temporal_layers": ["ndmi"],
                        "official_layers": ["coneat"],
                        "scope_type": "nacional",
                        "scope_ref": "Uruguay",
                        "timeline_scope": "nacional",
                        "target_date": "2026-04-03",
                    },
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_key"], "preload-1")

    def test_v1_preload_status_contract(self):
        payload = {
            "run_key": "preload-1",
            "run_type": "startup",
            "status": "running",
            "progress_total": 12,
            "progress_done": 6,
            "stage": "analytic_neighbors",
            "details": {"critical_ready": True},
            "critical_ready": True,
            "task_state": "running",
        }
        with patch("app.api.v1.endpoints.public.get_preload_status", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/preload/status?run_key=preload-1")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["critical_ready"])

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

    def test_v1_alert_subscriptions_contract(self):
        payload = [
            {
                "id": "subcfg-1",
                "scope_type": "department",
                "scope_id": "department-rivera",
                "scope_label": "Rivera",
                "channels_json": ["email", "whatsapp"],
                "min_alert_state": "Alerta",
                "active": True,
                "last_sent_state": "Vigilancia",
                "last_sent_at": "2026-03-30T00:00:00+00:00",
                "metadata_extra": {},
            }
        ]
        with patch("app.api.v1.endpoints.alert_subscriptions.notification_service.list_alert_subscriptions", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/alert-subscriptions")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total"], 1)
        self.assertEqual(response.json()["items"][0]["scope_label"], "Rivera")

    def test_v1_alert_subscriptions_options_contract(self):
        payload = {
            "scope_types": [{"value": "field", "label": "Campo"}, {"value": "national", "label": "Pais"}],
            "min_alert_states": [{"value": "Alerta", "label": "Alerta"}],
            "channels": [{"value": "email", "label": "Email", "enabled": True, "reason": None}],
            "national": {"value": "national", "label": "Uruguay"},
            "departments": [{"id": "department-rivera", "label": "Rivera", "department": "Rivera"}],
            "productive_units": [{"id": "productive-predio-demo", "label": "Predio Demo", "department": "Rivera", "unit_category": "predio"}],
            "fields": [{"id": "farm-field-1", "label": "Campo Norte", "department": "Rivera", "establishment_id": "farm-est-1", "establishment_name": "Estancia Demo", "aoi_unit_id": "productive-predio-demo"}],
            "contact": {"email": "demo@example.com", "whatsapp_e164": "+59899111222"},
        }
        with patch("app.api.v1.endpoints.alert_subscriptions.notification_service.get_alert_subscription_options", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/alert-subscriptions/options")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["national"]["label"], "Uruguay")
        self.assertEqual(response.json()["fields"][0]["label"], "Campo Norte")

    def test_v1_alert_subscriptions_test_send_contract(self):
        payload = {
            "status": "sent",
            "reason": "manual_test",
            "results": [{"channel": "email", "status": "sent", "id": "notif-1"}],
        }
        with patch("app.api.v1.endpoints.alert_subscriptions.notification_service.send_alert_subscription_test", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/alert-subscriptions/subcfg-1/test-send")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["reason"], "manual_test")

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

    def test_v1_campos_options_contract(self):
        payload = {
            "departments": [{"id": "department-rivera", "label": "Rivera"}],
            "establishments": [{"id": "farm-est-1", "name": "Estancia Demo", "description": "Test", "active": True}],
        }
        with patch("app.api.v1.endpoints.campos.get_farm_options", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.get("/api/v1/campos/options")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["departments"][0]["label"], "Rivera")

    def test_v1_establecimientos_create_contract(self):
        payload = {"id": "farm-est-1", "name": "Estancia Demo", "description": "Test", "active": True}
        with patch("app.api.v1.endpoints.campos.save_establishment", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post("/api/v1/establecimientos", json={"name": "Estancia Demo", "description": "Test"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["id"], "farm-est-1")

    def test_v1_campos_create_contract(self):
        payload = {
            "id": "farm-field-1",
            "establishment_id": "farm-est-1",
            "establishment_name": "Estancia Demo",
            "name": "Campo Norte",
            "department": "Rivera",
            "padron_value": "12345",
            "field_geometry_geojson": {"type": "Polygon", "coordinates": []},
            "aoi_unit_id": "productive-campo-norte",
        }
        with patch("app.api.v1.endpoints.campos.save_field", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/campos",
                    json={
                        "establishment_id": "farm-est-1",
                        "name": "Campo Norte",
                        "department": "Rivera",
                        "padron_value": "12345",
                        "field_geometry_geojson": {"type": "Polygon", "coordinates": []},
                    },
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "Campo Norte")

    def test_v1_potreros_create_contract(self):
        payload = {
            "id": "farm-paddock-1",
            "field_id": "farm-field-1",
            "name": "Potrero Norte",
            "geometry_geojson": {"type": "Polygon", "coordinates": []},
            "area_ha": 8.2,
            "display_order": 1,
            "active": True,
        }
        with patch("app.api.v1.endpoints.campos.save_paddock", new=AsyncMock(return_value=payload)):
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/campos/farm-field-1/potreros",
                    json={"name": "Potrero Norte", "geometry_geojson": {"type": "Polygon", "coordinates": []}},
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["field_id"], "farm-field-1")

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
