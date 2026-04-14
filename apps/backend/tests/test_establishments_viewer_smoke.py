import asyncio
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

TEST_DB = Path(__file__).resolve().parent / f"test_establishments_viewer_{uuid.uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from fastapi.testclient import TestClient

from app.db.session import AsyncSessionLocal, Base, engine
from app.main import app
from app.models.auth import AppUser
from app.models.farm import FarmEstablishment, FarmField, FarmPaddock
from app.models.humedad import AOIUnit


async def _create_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _seed_minimal_farm_state() -> dict[str, str]:
    async with AsyncSessionLocal() as session:
        suffix = uuid.uuid4().hex[:10]
        user_id = "test-user"
        google_sub = "test-google-sub"
        establishment_id = f"farm-est-{suffix}"
        field_id = f"farm-field-{suffix}"
        paddock_id = f"farm-paddock-{suffix}"
        user = await session.get(AppUser, user_id)
        if user is None:
            user = AppUser(
                id=user_id,
                google_sub=google_sub,
                email="test@agroclimax.local",
                email_verified=True,
                full_name="Test User",
                is_active=True,
            )
            session.add(user)

        establishment = FarmEstablishment(
            id=establishment_id,
            user_id=user_id,
            name="Estancia Smoke",
            description="Seeded by tests",
            active=True,
        )
        session.add(establishment)

        field_unit_id = f"user-field-{field_id}"
        field_geom = {
            "type": "Polygon",
            "coordinates": [[[-56.25, -31.55], [-56.05, -31.55], [-56.05, -31.35], [-56.25, -31.35], [-56.25, -31.55]]],
        }
        field_unit = AOIUnit(
            id=field_unit_id,
            slug=field_unit_id,
            unit_type="productive_unit",
            scope="unidad",
            name="Campo Smoke",
            department="Rivera",
            geometry_geojson=field_geom,
            centroid_lat=-31.45,
            centroid_lon=-56.15,
            source="user_field",
            data_mode="derived_department",
            metadata_extra={"unit_category": "campo"},
            active=True,
        )
        session.add(field_unit)

        field = FarmField(
            id=field_id,
            establishment_id=establishment.id,
            user_id=user_id,
            name="Campo Smoke",
            department="Rivera",
            padron_value="12345",
            field_geometry_geojson=field_geom,
            centroid_lat=-31.45,
            centroid_lon=-56.15,
            area_ha=120.0,
            aoi_unit_id=field_unit_id,
            active=True,
        )
        session.add(field)

        paddock_unit_id = f"user-paddock-{paddock_id}"
        paddock_geom = {
            "type": "Polygon",
            "coordinates": [[[-56.22, -31.52], [-56.12, -31.52], [-56.12, -31.42], [-56.22, -31.42], [-56.22, -31.52]]],
        }
        paddock_unit = AOIUnit(
            id=paddock_unit_id,
            slug=paddock_unit_id,
            unit_type="productive_unit",
            scope="unidad",
            name="Potrero 1",
            department="Rivera",
            geometry_geojson=paddock_geom,
            centroid_lat=-31.47,
            centroid_lon=-56.17,
            source="user_field",
            data_mode="derived_department",
            metadata_extra={"unit_category": "potrero"},
            active=True,
        )
        session.add(paddock_unit)

        paddock = FarmPaddock(
            id=paddock_id,
            field_id=field_id,
            user_id=user_id,
            name="Potrero 1",
            geometry_geojson=paddock_geom,
            area_ha=8.2,
            aoi_unit_id=paddock_unit_id,
            display_order=1,
            active=True,
        )
        session.add(paddock)

        await session.commit()
        return {
            "establishment_id": establishment.id,
            "field_id": field_id,
            "paddock_id": paddock_id,
        }


class EstablishmentViewerSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        asyncio.run(_create_tables())
        cls.ids = asyncio.run(_seed_minimal_farm_state())

    @classmethod
    def tearDownClass(cls):
        try:
            asyncio.run(engine.dispose())
        except Exception:
            pass
        try:
            TEST_DB.unlink(missing_ok=True)
        except Exception:
            pass

    def test_establishments_list_and_field_detail_shapes(self):
        # Keep these tests cheap and deterministic: patch analytics bundle so we only validate
        # the contracts that the viewer/map depend on.
        fake_bundle = {
            "field_payload": {"state": "Normal", "risk_score": 10.0, "analytics_mode": "direct_field"},
            "paddock_payloads": {},
            "analytics_mode": "direct_field",
        }
        with patch("app.services.farms._ensure_field_analytics_bundle", new=AsyncMock(return_value=fake_bundle)):
            with TestClient(app) as client:
                resp = client.get("/api/v1/establecimientos")
                self.assertEqual(resp.status_code, 200)
                payload = resp.json()
                self.assertIn("total", payload)
                self.assertIn("items", payload)
                self.assertGreaterEqual(payload["total"], 1)

                campos = client.get(f"/api/v1/campos?establishment_id={self.ids['establishment_id']}")
                self.assertEqual(campos.status_code, 200)
                campos_payload = campos.json()
                self.assertIn("total", campos_payload)
                self.assertIn("items", campos_payload)
                self.assertGreaterEqual(campos_payload["total"], 1)
                item = campos_payload["items"][0]
                # Viewer needs basic metadata + geometry linkage.
                self.assertIn("id", item)
                self.assertIn("name", item)
                self.assertIn("department", item)
                self.assertIn("establishment_id", item)
                self.assertIn("establishment_name", item)
                self.assertIn("field_geometry_geojson", item)
                self.assertIn("aoi_unit_id", item)

                field_detail = client.get(f"/api/v1/campos/{self.ids['field_id']}")
                self.assertEqual(field_detail.status_code, 200)
                detail = field_detail.json()
                self.assertEqual(detail["id"], self.ids["field_id"])
                self.assertIn("paddocks", detail)
                self.assertTrue(isinstance(detail["paddocks"], list))
                # Paddocks should include geometry + aoi unit linkage for timeline scope.
                self.assertTrue(any(p.get("id") == self.ids["paddock_id"] for p in detail["paddocks"]))
                paddock = next(p for p in detail["paddocks"] if p.get("id") == self.ids["paddock_id"])
                self.assertIn("geometry_geojson", paddock)
                self.assertIn("aoi_unit_id", paddock)

                paddocks_geojson = client.get(f"/api/v1/campos/{self.ids['field_id']}/potreros/geojson")
                self.assertEqual(paddocks_geojson.status_code, 200)
                fc = paddocks_geojson.json()
                self.assertEqual(fc.get("type"), "FeatureCollection")
                self.assertIn("features", fc)
                self.assertTrue(any((f.get("properties") or {}).get("paddock_id") == self.ids["paddock_id"] for f in fc.get("features") or []))
