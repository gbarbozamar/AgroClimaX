import os
from datetime import datetime, timezone
from pathlib import Path
import time
from types import SimpleNamespace
import unittest
import asyncio
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

TEST_DB = Path(__file__).resolve().parent / f"test_alert_subscriptions_{uuid4().hex}.db"
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
from app.core.config import settings
from app.models.alerta import AlertState, AlertSubscription, AlertaEvento, NotificationMediaAsset
from app.models.auth import AppUser, AppUserProfile
from app.models.farm import FarmEstablishment, FarmField, FarmPaddock
from app.models.humedad import AOIUnit
from app.services.notification_media import create_notification_media_assets
from app.services.notifications import notification_service
from app.services.public_api import TRANSPARENT_PNG


class AlertSubscriptionTests(unittest.TestCase):
    def setUp(self):
        last_error = None
        for attempt in range(5):
            try:
                asyncio.run(self._reset_state())
                return
            except OperationalError as exc:
                last_error = exc
                asyncio.run(engine.dispose())
                time.sleep(0.25 * (attempt + 1))
        raise last_error

    def tearDown(self):
        asyncio.run(engine.dispose())

    async def _reset_state(self):
        await engine.dispose()
        for attempt in range(5):
            try:
                if TEST_DB.exists():
                    TEST_DB.unlink()
                break
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.25 * (attempt + 1))
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
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
                await session.flush()

    async def _seed_field_scope(self):
        async with AsyncSessionLocal() as session:
            establishment = FarmEstablishment(
                id="farm-est-1",
                user_id="test-user",
                name="Establecimiento Test",
                active=True,
            )
            field = FarmField(
                id="farm-field-1",
                establishment_id=establishment.id,
                user_id="test-user",
                name="Campo Test",
                department="Rivera",
                padron_value="12345",
                padron_source="snig_padronario_rural",
                padron_lookup_payload={"properties": {"PADRON": 12345}},
                padron_geometry_geojson={
                    "type": "Polygon",
                    "coordinates": [[[-55.8, -31.35], [-55.74, -31.35], [-55.74, -31.4], [-55.8, -31.4], [-55.8, -31.35]]],
                },
                field_geometry_geojson={
                    "type": "Polygon",
                    "coordinates": [[[-55.8, -31.35], [-55.74, -31.35], [-55.74, -31.4], [-55.8, -31.4], [-55.8, -31.35]]],
                },
                centroid_lat=-31.375,
                centroid_lon=-55.77,
                area_ha=24.0,
                aoi_unit_id="productive-test-unit",
                active=True,
            )
            paddock = FarmPaddock(
                id="farm-paddock-1",
                field_id=field.id,
                user_id="test-user",
                name="Potrero Norte",
                geometry_geojson={
                    "type": "Polygon",
                    "coordinates": [[[-55.795, -31.355], [-55.77, -31.355], [-55.77, -31.385], [-55.795, -31.385], [-55.795, -31.355]]],
                },
                area_ha=8.0,
                display_order=1,
                active=True,
            )
            session.add_all([establishment, field, paddock])
            await session.commit()
            return field.id

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

    def test_manual_test_forces_dispatch_even_if_threshold_is_higher(self):
        with TestClient(app) as client:
            create_response = client.post(
                "/api/v1/alert-subscriptions",
                json={
                    "scope_type": "productive_unit",
                    "scope_id": "productive-test-unit",
                    "channels_json": ["email"],
                    "min_alert_state": "Emergencia",
                    "active": False,
                },
            )
            self.assertEqual(create_response.status_code, 200)
            subscription_id = create_response.json()["id"]

            mocked = AsyncMock(return_value={"status": "sent", "reason": "manual_test", "results": []})
            with patch.object(notification_service, "_dispatch_configurable_scope_subscriptions", mocked):
                response = client.post(f"/api/v1/alert-subscriptions/{subscription_id}/test-send")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(mocked.await_args.kwargs["force_dispatch"])
        self.assertEqual(mocked.await_args.kwargs["subscription_ids"], [subscription_id])

    def test_send_twilio_message_uses_sync_httpx_post(self):
        async def _run():
            async with AsyncSessionLocal() as session:
                with (
                    patch.object(settings, "twilio_account_sid", "AC_test"),
                    patch.object(settings, "twilio_auth_token", "token_test"),
                    patch.object(settings, "twilio_whatsapp_from", "whatsapp:+14155238886"),
                    patch(
                        "app.services.notifications.httpx.post",
                        side_effect=[
                            SimpleNamespace(
                                status_code=201,
                                text='{"sid":"SM123","status":"queued"}',
                                is_success=True,
                                json=lambda: {"sid": "SM123", "status": "queued"},
                            ),
                            SimpleNamespace(
                                status_code=201,
                                text='{"sid":"MM124","status":"queued"}',
                                is_success=True,
                                json=lambda: {"sid": "MM124", "status": "queued"},
                            ),
                            SimpleNamespace(
                                status_code=201,
                                text='{"sid":"MM125","status":"queued"}',
                                is_success=True,
                                json=lambda: {"sid": "MM125", "status": "queued"},
                            ),
                        ],
                    ) as mocked_post,
                    patch(
                        "app.services.notifications.httpx.get",
                        side_effect=[
                            SimpleNamespace(
                                status_code=200,
                                text='{"sid":"SM123","status":"delivered"}',
                                is_success=True,
                                json=lambda: {"sid": "SM123", "status": "delivered"},
                            ),
                            SimpleNamespace(
                                status_code=200,
                                text='{"sid":"MM124","status":"delivered"}',
                                is_success=True,
                                json=lambda: {"sid": "MM124", "status": "delivered"},
                            ),
                            SimpleNamespace(
                                status_code=200,
                                text='{"sid":"MM125","status":"delivered"}',
                                is_success=True,
                                json=lambda: {"sid": "MM125", "status": "delivered"},
                            ),
                        ],
                    ) as mocked_get,
                    patch("app.services.notifications.asyncio.sleep", new=AsyncMock()),
                ):
                    result = await notification_service._send_twilio_message(
                        session,
                        alert_event_id=None,
                        channel="whatsapp",
                        recipient="+59899111222",
                        payload={
                            "body": "Prueba",
                            "media_assets": [
                                {"url": "https://example.com/a.png", "kind": "alert_overview"},
                                {"url": "https://example.com/b.png", "kind": "surface_soil_moisture"},
                            ],
                        },
                        reason="manual_test",
                    )
                return result, mocked_post.call_args_list, mocked_get.call_args_list

        result, call_args_list, get_call_args_list = asyncio.run(_run())
        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["provider_response"]["message_count"], 3)
        self.assertEqual(result["provider_response"]["sent_count"], 3)
        self.assertEqual(len(call_args_list), 3)
        self.assertEqual(len(get_call_args_list), 3)

        first_call = call_args_list[0]
        second_call = call_args_list[1]
        third_call = call_args_list[2]

        self.assertNotIn(b"MediaUrl=", first_call.kwargs["content"])
        self.assertIn(b"Body=Prueba", first_call.kwargs["content"])

        self.assertIn(b"Body=Mapa+de+alerta", second_call.kwargs["content"])
        self.assertIn(b"MediaUrl=https%3A%2F%2Fexample.com%2Fa.png", second_call.kwargs["content"])

        self.assertIn(b"Body=Humedad+Superficial+del+Suelo", third_call.kwargs["content"])
        self.assertIn(b"MediaUrl=https%3A%2F%2Fexample.com%2Fb.png", third_call.kwargs["content"])

        for call in call_args_list:
            self.assertEqual(call.kwargs["headers"]["Content-Type"], "application/x-www-form-urlencoded")

        self.assertTrue(get_call_args_list[0].kwargs["url"].endswith("/SM123.json"))
        self.assertTrue(get_call_args_list[1].kwargs["url"].endswith("/MM124.json"))
        self.assertTrue(get_call_args_list[2].kwargs["url"].endswith("/MM125.json"))

    def test_alert_subscription_options_include_field_scope(self):
        with TestClient(app) as client:
            establishment_response = client.post(
                "/api/v1/establecimientos",
                json={"name": "Establecimiento Test", "description": "Demo"},
            )
            self.assertEqual(establishment_response.status_code, 200)
            create_field_response = client.post(
                "/api/v1/campos",
                json={
                    "establishment_id": establishment_response.json()["id"],
                    "name": "Campo Test",
                    "department": "Rivera",
                    "padron_value": "12345",
                    "padron_source": "snig_padronario_rural",
                    "padron_lookup_payload": {"properties": {"PADRON": 12345}},
                    "padron_geometry_geojson": {
                        "type": "Polygon",
                        "coordinates": [[[-55.8, -31.35], [-55.74, -31.35], [-55.74, -31.4], [-55.8, -31.4], [-55.8, -31.35]]],
                    },
                    "field_geometry_geojson": {
                        "type": "Polygon",
                        "coordinates": [[[-55.8, -31.35], [-55.74, -31.35], [-55.74, -31.4], [-55.8, -31.4], [-55.8, -31.35]]],
                    },
                },
            )
            self.assertEqual(create_field_response.status_code, 200)
            response = client.get("/api/v1/alert-subscriptions/options")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(any(item["value"] == "field" for item in body["scope_types"]))
        self.assertEqual(body["fields"][0]["label"], "Campo Test")

    def test_dispatch_operational_alerts_dispatches_field_scope_for_user_field(self):
        asyncio.run(self._seed_field_scope())

        async def _run():
            async with AsyncSessionLocal() as session:
                unit = await session.get(AOIUnit, "productive-test-unit")
                current_state = AlertState(
                    unit_id=unit.id,
                    scope="unidad",
                    department="Rivera",
                    observed_at=datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
                    current_state="Vigilancia",
                    state_level=1,
                    risk_score=58.0,
                    confidence_score=72.0,
                    affected_pct=14.0,
                    days_in_state=3,
                    data_mode="live_copernicus",
                    drivers=[{"name": "vulnerabilidad_suelo"}],
                    forecast=[],
                )
                alert_event = AlertaEvento(
                    id="alert-field-1",
                    unit_id=unit.id,
                    fecha=datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
                    departamento="Rivera",
                    nivel=1,
                    nivel_nombre="Vigilancia",
                )
                mocked = AsyncMock(
                    side_effect=[
                        {"status": "sent", "results": [{"channel": "email", "status": "sent"}]},
                        {"status": "sent", "results": [{"channel": "whatsapp", "status": "sent"}]},
                    ]
                )
                with patch.object(notification_service, "_dispatch_configurable_scope_subscriptions", mocked):
                    result = await notification_service.dispatch_operational_alerts(
                        session,
                        unit=unit,
                        alert_event=alert_event,
                        current_state=current_state,
                        previous_state={"current_state": "Normal", "state_level": 0, "confidence_score": 60.0, "forecast": []},
                    )
                return result, mocked.await_args_list

        result, await_args_list = asyncio.run(_run())
        self.assertEqual(result["status"], "sent")
        self.assertEqual(len(await_args_list), 2)
        self.assertEqual(await_args_list[0].kwargs["scope_type"], "productive_unit")
        self.assertEqual(await_args_list[1].kwargs["scope_type"], "field")
        self.assertEqual(await_args_list[1].kwargs["scope_id"], "farm-field-1")
        self.assertEqual(await_args_list[1].kwargs["scope_label"], "Campo Test")
        self.assertEqual(await_args_list[1].kwargs["overlay_features"][0]["label"], "Potrero Norte")
