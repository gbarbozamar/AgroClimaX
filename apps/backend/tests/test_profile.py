import os
from pathlib import Path
import unittest
import asyncio
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import delete

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.main import app
from app.db.session import AsyncSessionLocal
from app.models.auth import AppUserProfile


COMPLETE_PROFILE = {
    "phone_e164": "+59899111222",
    "whatsapp_e164": "+59899111222",
    "organization_name": "Establecimiento Demo",
    "organization_type": "productor",
    "role_code": "productor",
    "job_title": "Director",
    "scope_type": "nacional",
    "scope_ids_json": [],
    "production_type": "ganaderia",
    "operation_size_hectares": 420.5,
    "livestock_headcount": 320,
    "crop_types_json": ["pasturas", "maiz"],
    "use_cases_json": ["monitoreo_diario", "gestion_alertas"],
    "alert_channels_json": ["email", "whatsapp"],
    "min_alert_state": "Alerta",
    "preferred_language": "es-UY",
    "communications_opt_in": True,
    "data_usage_consent": True,
}


class ProfileEndpointTests(unittest.TestCase):
    def setUp(self):
        asyncio.run(self._reset_profiles())

    async def _reset_profiles(self):
        async with AsyncSessionLocal() as session:
            await session.execute(delete(AppUserProfile))
            await session.commit()

    def test_profile_me_returns_google_identity_and_options(self):
        with TestClient(app) as client:
            response = client.get("/api/v1/profile/me")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["google_identity"]["email"], "test@agroclimax.local")
        self.assertIn("organization_types", payload["options"])
        self.assertLess(payload["completion"]["completion_pct"], 100)

    def test_profile_save_marks_profile_complete(self):
        with TestClient(app) as client:
            response = client.put("/api/v1/profile/me", json=COMPLETE_PROFILE)
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertTrue(payload["completion"]["is_complete"])
            self.assertEqual(payload["profile"]["organization_type"], "productor")

            reloaded = client.get("/api/v1/profile/me")
            self.assertEqual(reloaded.status_code, 200)
            self.assertTrue(reloaded.json()["completion"]["is_complete"])

    def test_profile_rejects_invalid_phone_format(self):
        invalid_payload = {**COMPLETE_PROFILE, "phone_e164": "099111222", "alert_channels_json": ["sms"]}
        with TestClient(app) as client:
            response = client.put("/api/v1/profile/me", json=invalid_payload)
        self.assertEqual(response.status_code, 422)
        self.assertIn("E.164", response.json()["detail"])

    def test_profile_accepts_empty_numeric_strings(self):
        payload = {
            **COMPLETE_PROFILE,
            "operation_size_hectares": "",
            "livestock_headcount": "",
        }
        with TestClient(app) as client:
            response = client.put("/api/v1/profile/me", json=payload)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIsNone(body["profile"]["operation_size_hectares"])
        self.assertIsNone(body["profile"]["livestock_headcount"])

    def test_profile_requires_auth_when_bypass_disabled(self):
        with patch("app.services.auth.settings.auth_bypass_for_tests", False):
            with TestClient(app) as client:
                response = client.get("/api/v1/profile/me")
        self.assertEqual(response.status_code, 401)
