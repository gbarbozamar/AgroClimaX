import os
from datetime import datetime, timezone
from pathlib import Path
import unittest
import asyncio
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from sqlalchemy import delete, select

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.main import app
from app.db.session import AsyncSessionLocal, Base, engine
from app.models.alerta import AlertSubscription, NotificationMediaAsset
from app.models.auth import AppUserProfile
from app.models.humedad import AOIUnit
from app.services.notification_media import create_notification_media_assets
from app.services.public_api import TRANSPARENT_PNG


class AlertSubscriptionTests(unittest.TestCase):
    def setUp(self):
        asyncio.run(self._reset_state())

    async def _reset_state(self):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with AsyncSessionLocal() as session:
            await session.execute(delete(NotificationMediaAsset))
            await session.execute(delete(AlertSubscription))
            await session.execute(delete(AppUserProfile))
            await session.execute(delete(AOIUnit).where(AOIUnit.id == "productive-test-unit"))
            session.add(
                AOIUnit(
                    id="productive-test-unit",
                    slug="productive-test-unit",
                    unit_type="productive_unit",
                    scope="unidad",
                    name="Predio Test",
                    department="Rivera",
                    geometry_geojson={
                        "type": "Polygon",
                        "coordinates": [[[-55.8, -31.35], [-55.75, -31.35], [-55.75, -31.39], [-55.8, -31.39], [-55.8, -31.35]]],
                    },
                    centroid_lat=-31.37,
                    centroid_lon=-55.775,
                    metadata_extra={"unit_category": "predio"},
                )
            )
            await session.commit()

    def _first_department_id(self, client: TestClient) -> str:
        payload = client.get("/api/v1/alert-subscriptions/options").json()
        return payload["departments"][0]["id"]

    def test_create_national_email_subscription(self):
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/alert-subscriptions",
                json={
                    "scope_type": "national",
                    "channels_json": ["email"],
                    "min_alert_state": "Alerta",
                    "active": True,
                },
            )
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["scope_label"], "Uruguay")

            listed = client.get("/api/v1/alert-subscriptions")
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(listed.json()["total"], 1)

    def test_whatsapp_requires_profile_number(self):
        with TestClient(app) as client:
            department_id = self._first_department_id(client)
            response = client.post(
                "/api/v1/alert-subscriptions",
                json={
                    "scope_type": "department",
                    "scope_id": department_id,
                    "channels_json": ["whatsapp"],
                    "min_alert_state": "Alerta",
                    "active": True,
                },
            )
        self.assertEqual(response.status_code, 422)
        self.assertIn("WhatsApp", response.json()["detail"])

    def test_public_asset_endpoint_serves_png(self):
        async def _create_asset():
            async with AsyncSessionLocal() as session:
                with patch("app.services.notification_media.fetch_tile_png", new=AsyncMock(return_value=TRANSPARENT_PNG)):
                    assets = await create_notification_media_assets(
                        session,
                        scope_type="productive_unit",
                        scope_id="productive-test-unit",
                        scope_label="Predio Test",
                        geometry_geojson={
                            "type": "Polygon",
                            "coordinates": [[[-55.8, -31.35], [-55.75, -31.35], [-55.75, -31.39], [-55.8, -31.39], [-55.8, -31.35]]],
                        },
                        state_name="Alerta",
                        observed_at=datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
                        department="Rivera",
                        risk_score=71.2,
                        confidence_score=78.4,
                        affected_pct=24.0,
                    )
                await session.commit()
                return assets[0]

        asset = asyncio.run(_create_asset())
        with TestClient(app) as client:
            response = client.get(f"/api/v1/alert-subscriptions/assets/{asset['id']}?token={asset['url'].split('token=')[1]}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/png")
        self.assertTrue(response.content.startswith(b"\x89PNG"))
