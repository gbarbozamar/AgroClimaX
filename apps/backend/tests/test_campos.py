import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest
import asyncio
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

TEST_DB = Path(__file__).resolve().parent / f"test_campos_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"

from app.main import app
from app.db.session import AsyncSessionLocal, Base, engine
from app.models.alerta import AlertState
from app.models.auth import AppUser
from app.models.farm import FarmEstablishment, FarmField, FarmPaddock, PadronLookupCache
from app.models.humedad import AOIUnit


FIELD_GEOMETRY = {
    "type": "Polygon",
    "coordinates": [[[-55.8, -31.35], [-55.74, -31.35], [-55.74, -31.4], [-55.8, -31.4], [-55.8, -31.35]]],
}

PADRON_GEOMETRY = {
    "type": "Polygon",
    "coordinates": [[[-55.82, -31.34], [-55.72, -31.34], [-55.72, -31.41], [-55.82, -31.41], [-55.82, -31.34]]],
}

PADDOCK_A = {
    "type": "Polygon",
    "coordinates": [[[-55.795, -31.355], [-55.77, -31.355], [-55.77, -31.385], [-55.795, -31.385], [-55.795, -31.355]]],
}

PADDOCK_B_OVERLAP = {
    "type": "Polygon",
    "coordinates": [[[-55.782, -31.36], [-55.752, -31.36], [-55.752, -31.39], [-55.782, -31.39], [-55.782, -31.36]]],
}

PADDOCK_B = {
    "type": "Polygon",
    "coordinates": [[[-55.766, -31.355], [-55.745, -31.355], [-55.745, -31.385], [-55.766, -31.385], [-55.766, -31.355]]],
}

PADDOCK_OUTSIDE = {
    "type": "Polygon",
    "coordinates": [[[-55.72, -31.33], [-55.7, -31.33], [-55.7, -31.35], [-55.72, -31.35], [-55.72, -31.33]]],
}

PADDOCK_WITHIN_BUFFER = {
    "type": "Polygon",
    "coordinates": [[[-55.742, -31.36], [-55.73993, -31.36], [-55.73993, -31.38], [-55.742, -31.38], [-55.742, -31.36]]],
}


class CamposEndpointTests(unittest.TestCase):
    def setUp(self):
        self._analysis_patch = patch("app.services.farms.ensure_latest_daily_analysis", new=AsyncMock(return_value={"status": "ok"}))
        self._analyze_patch = patch("app.services.farms.analyze_unit", new=AsyncMock(side_effect=self._mock_analyze_unit))
        self._analysis_patch.start()
        self._analyze_patch.start()
        asyncio.run(self._reset_state())

    def tearDown(self):
        self._analyze_patch.stop()
        self._analysis_patch.stop()
        asyncio.run(engine.dispose())

    async def _mock_analyze_unit(self, session, *, unit, target_date, geojson=None, **kwargs):
        del session, target_date, geojson, kwargs
        metadata = unit.metadata_extra or {}
        unit_category = str(metadata.get("unit_category") or "").strip().lower()
        paddock_name = (unit.name or "").strip().lower()
        if unit_category == "potrero":
            if "norte" in paddock_name or paddock_name.endswith("a"):
                return {
                    "state": AlertState(
                        unit_id=unit.id,
                        scope=unit.scope,
                        department=unit.department,
                        observed_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                        current_state="Vigilancia",
                        state_level=1,
                        risk_score=42.0,
                        confidence_score=70.0,
                        affected_pct=18.0,
                        largest_cluster_pct=9.0,
                        days_in_state=4,
                        actionable=False,
                        data_mode="derived_department",
                        drivers=[{"name": "humedad_superficial", "score": 42.0, "detail": "Potrero mas humedo"}],
                        forecast=[{"date": "2026-04-01", "expected_risk": 45.0, "temp_max_c": 28.0, "precip_mm": 2.0}],
                        soil_context={"soil_label": "loma"},
                        calibration_ref="test-v1",
                        raw_metrics={
                            "s1_humidity_mean_pct": 36.0,
                            "s1_vv_db_mean": -10.2,
                            "s2_ndmi_mean": 0.22,
                            "estimated_ndmi": 0.24,
                            "spi_30d": -0.6,
                            "component_scores": {"soil": 40.0, "weather": 44.0},
                        },
                        explanation="Mock paddock A",
                        metadata_extra={"rules_version": "test-v1"},
                    )
                }
            return {
                "state": AlertState(
                    unit_id=unit.id,
                    scope=unit.scope,
                    department=unit.department,
                    observed_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                    current_state="Alerta",
                    state_level=2,
                    risk_score=76.0,
                    confidence_score=82.0,
                    affected_pct=41.0,
                    largest_cluster_pct=19.0,
                    days_in_state=7,
                    actionable=True,
                    data_mode="derived_department",
                    drivers=[{"name": "deficit_hidrico", "score": 76.0, "detail": "Potrero mas seco"}],
                    forecast=[{"date": "2026-04-01", "expected_risk": 79.0, "temp_max_c": 31.0, "precip_mm": 0.5}],
                    soil_context={"soil_label": "planicie"},
                    calibration_ref="test-v1",
                    raw_metrics={
                        "s1_humidity_mean_pct": 21.0,
                        "s1_vv_db_mean": -14.4,
                        "s2_ndmi_mean": -0.08,
                        "estimated_ndmi": -0.05,
                        "spi_30d": -1.4,
                        "component_scores": {"soil": 78.0, "weather": 74.0},
                    },
                    explanation="Mock paddock B",
                    metadata_extra={"rules_version": "test-v1"},
                )
            }
        return {
            "state": AlertState(
                unit_id=unit.id,
                scope=unit.scope,
                department=unit.department,
                observed_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                current_state="Alerta",
                state_level=2,
                risk_score=63.0,
                confidence_score=75.0,
                affected_pct=28.0,
                largest_cluster_pct=14.0,
                days_in_state=5,
                actionable=True,
                data_mode="derived_department",
                drivers=[{"name": "base_campo", "score": 63.0, "detail": "Mock campo"}],
                forecast=[{"date": "2026-04-01", "expected_risk": 67.0, "temp_max_c": 29.0, "precip_mm": 1.2}],
                soil_context={"soil_label": "campo"},
                calibration_ref="test-v1",
                raw_metrics={
                    "s1_humidity_mean_pct": 29.0,
                    "s1_vv_db_mean": -11.8,
                    "s2_ndmi_mean": 0.04,
                    "estimated_ndmi": 0.05,
                    "spi_30d": -0.9,
                    "component_scores": {"soil": 64.0, "weather": 62.0},
                },
                explanation="Mock field",
                metadata_extra={"rules_version": "test-v1"},
            )
        }

    async def _reset_state(self):
        await engine.dispose()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("PRAGMA foreign_keys=OFF"))
            for table in reversed(Base.metadata.sorted_tables):
                await conn.execute(text(f'DELETE FROM "{table.name}"'))
            await conn.execute(text("PRAGMA foreign_keys=ON"))
            async with AsyncSession(bind=conn, expire_on_commit=False) as session:
                session.add(
                    AppUser(
                        id="test-user",
                        google_sub="test-google-sub",
                        email="test@agroclimax.local",
                        email_verified=True,
                        full_name="Test User",
                        given_name="Test",
                        family_name="User",
                        locale="es-UY",
                        is_active=True,
                    )
                )
                session.add(
                    AOIUnit(
                        id="department-rivera",
                        slug="department-rivera",
                        unit_type="department",
                        scope="departamento",
                        name="Rivera",
                        department="Rivera",
                        geometry_geojson=PADRON_GEOMETRY,
                        centroid_lat=-31.375,
                        centroid_lon=-55.77,
                        metadata_extra={},
                        active=True,
                    )
                )
                await session.flush()

    def _create_establishment(self, client: TestClient) -> str:
        response = client.post(
            "/api/v1/establecimientos",
            json={"name": "Establecimiento Norte", "description": "Demo"},
        )
        self.assertEqual(response.status_code, 200)
        return response.json()["id"]

    def _create_field(self, client: TestClient, establishment_id: str) -> dict:
        response = client.post(
            "/api/v1/campos",
            json={
                "establishment_id": establishment_id,
                "name": "Campo Uno",
                "department": "Rivera",
                "padron_value": "12345",
                "padron_source": "snig_padronario_rural",
                "padron_lookup_payload": {"properties": {"PADRON": 12345, "DEPTO": "Rivera"}},
                "padron_geometry_geojson": PADRON_GEOMETRY,
                "field_geometry_geojson": FIELD_GEOMETRY,
            },
        )
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_padron_search_returns_feature_and_uses_cache(self):
        fake_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": PADRON_GEOMETRY,
                        "properties": {"PADRON": 1, "DEPTO": "CERRO LARGO", "AREAHA": 42.5, "DEPTOPADRON": "CERRO LARGO"},
                    }
                ],
            },
        )
        with patch("app.services.farms.httpx.AsyncClient.get", new=AsyncMock(return_value=fake_response)):
            with TestClient(app) as client:
                first = client.get("/api/v1/padrones/search?department=Cerro%20Largo&padron=1")
                second = client.get("/api/v1/padrones/search?department=Cerro%20Largo&padron=1")
        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.json()["found"])
        self.assertFalse(first.json()["cached"])
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.json()["found"])
        self.assertTrue(second.json()["cached"])

    def test_create_field_creates_linked_aoi_unit(self):
        with TestClient(app) as client:
            establishment_id = self._create_establishment(client)
            field = self._create_field(client, establishment_id)
            self.assertEqual(field["analytics_mode"], "direct_field")
            self.assertIsNotNone(field["field_analytics"])
            self.assertEqual(field["field_analytics"]["state"], "Alerta")
            self.assertEqual(field["field_analytics"]["risk_score"], 63.0)

        async def _assertions():
            async with AsyncSessionLocal() as session:
                stored_field = await session.get(FarmField, field["id"])
                self.assertIsNotNone(stored_field)
                self.assertTrue(stored_field.aoi_unit_id)
                aoi_unit = await session.get(AOIUnit, stored_field.aoi_unit_id)
                self.assertIsNotNone(aoi_unit)
                self.assertEqual(aoi_unit.unit_type, "productive_unit")
                self.assertEqual((aoi_unit.metadata_extra or {}).get("source"), "user_field")
                self.assertEqual((aoi_unit.metadata_extra or {}).get("farm_field_id"), field["id"])

        asyncio.run(_assertions())

    def test_paddock_creates_own_aoi_unit_and_field_aggregates_weighted_analytics(self):
        with TestClient(app) as client:
            establishment_id = self._create_establishment(client)
            field = self._create_field(client, establishment_id)
            first = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero Norte", "geometry_geojson": PADDOCK_A},
            )
            self.assertEqual(first.status_code, 200)
            second = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero Sur", "geometry_geojson": PADDOCK_B},
            )
            self.assertEqual(second.status_code, 200)
            detail = client.get(f"/api/v1/campos/{field['id']}")
            self.assertEqual(detail.status_code, 200)
            body = detail.json()

        self.assertEqual(body["analytics_mode"], "paddock_weighted")
        self.assertEqual(len(body["paddocks"]), 2)
        self.assertTrue(body["paddocks"][0]["aoi_unit_id"])
        self.assertTrue(body["paddocks"][1]["aoi_unit_id"])
        paddock_risks = [item["paddock_analytics"]["risk_score"] for item in body["paddocks"]]
        paddock_areas = [float(item["area_ha"]) for item in body["paddocks"]]
        expected_risk = round(sum(risk * area for risk, area in zip(paddock_risks, paddock_areas)) / sum(paddock_areas), 1)
        self.assertEqual(body["field_analytics"]["risk_score"], expected_risk)
        self.assertEqual(body["field_analytics"]["analytics_mode"], "paddock_weighted")
        self.assertIn(body["field_analytics"]["primary_driver"], {"humedad_superficial", "deficit_hidrico"})

        async def _assertions():
            async with AsyncSessionLocal() as session:
                paddocks = list(
                    (
                        await session.execute(
                            select(FarmPaddock).where(FarmPaddock.field_id == field["id"], FarmPaddock.active.is_(True))
                        )
                    ).scalars().all()
                )
                self.assertEqual(len(paddocks), 2)
                self.assertTrue(all(item.aoi_unit_id for item in paddocks))
                for item in paddocks:
                    aoi_unit = await session.get(AOIUnit, item.aoi_unit_id)
                    self.assertIsNotNone(aoi_unit)
                    self.assertEqual((aoi_unit.metadata_extra or {}).get("unit_category"), "potrero")
                    self.assertEqual((aoi_unit.metadata_extra or {}).get("farm_paddock_id"), item.id)

        asyncio.run(_assertions())

    def test_geojson_endpoints_include_embedded_analytics(self):
        with TestClient(app) as client:
            establishment_id = self._create_establishment(client)
            field = self._create_field(client, establishment_id)
            paddock = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero Norte", "geometry_geojson": PADDOCK_A},
            )
            self.assertEqual(paddock.status_code, 200)
            fields_geojson = client.get("/api/v1/campos/geojson")
            paddocks_geojson = client.get(f"/api/v1/campos/{field['id']}/potreros/geojson")

        self.assertEqual(fields_geojson.status_code, 200)
        self.assertEqual(paddocks_geojson.status_code, 200)
        field_feature = fields_geojson.json()["features"][0]
        paddock_feature = paddocks_geojson.json()["features"][0]
        self.assertIn("analytics", field_feature["properties"])
        self.assertIn("analytics_mode", field_feature["properties"])
        self.assertEqual(field_feature["properties"]["analytics"]["state"], "Vigilancia")
        self.assertIn("analytics", paddock_feature["properties"])
        self.assertEqual(paddock_feature["properties"]["analytics"]["state"], "Vigilancia")

    def test_paddock_allows_small_outside_tolerance(self):
        with TestClient(app) as client:
            establishment_id = self._create_establishment(client)
            field = self._create_field(client, establishment_id)
            response = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero borde", "geometry_geojson": PADDOCK_WITHIN_BUFFER},
            )
        self.assertEqual(response.status_code, 200)

    def test_paddock_must_be_inside_field_or_within_tolerance(self):
        with TestClient(app) as client:
            establishment_id = self._create_establishment(client)
            field = self._create_field(client, establishment_id)
            response = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero afuera", "geometry_geojson": PADDOCK_OUTSIDE},
            )
        self.assertEqual(response.status_code, 422)
        self.assertIn("m fuera del campo", response.json()["detail"])
        self.assertIn("tolerancia operativa de 10 m", response.json()["detail"])

    def test_paddocks_cannot_overlap(self):
        with TestClient(app) as client:
            establishment_id = self._create_establishment(client)
            field = self._create_field(client, establishment_id)
            first = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero A", "geometry_geojson": PADDOCK_A},
            )
            self.assertEqual(first.status_code, 200)
            second = client.post(
                f"/api/v1/campos/{field['id']}/potreros",
                json={"name": "Potrero B", "geometry_geojson": PADDOCK_B_OVERLAP},
            )
        self.assertEqual(second.status_code, 422)
        detail = second.json()["detail"]
        # Con tolerancia: el mensaje puede ser el legacy ("no pueden solaparse" para
        # contenimiento total) o el nuevo ("se solapa con 'Potrero A' en X m²").
        self.assertTrue(
            "no pueden solaparse" in detail or "se solapa con" in detail,
            f"Unexpected overlap error: {detail!r}",
        )


if __name__ == "__main__":
    unittest.main()
