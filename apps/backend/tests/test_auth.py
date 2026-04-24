import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.main import app
from app.models.auth import AppUser, AppUserProfile, AuthSession
from sqlalchemy import delete


async def _seed_real_auth_session(raw_token: str) -> tuple[str, str]:
    user_id = str(uuid.uuid4())
    csrf_token = "csrf-demo-token"
    async with AsyncSessionLocal() as session:
        user = AppUser(
            id=user_id,
            google_sub=f"google-{user_id}",
            email=f"{user_id}@example.com",
            email_verified=True,
            full_name="Usuario Demo",
            given_name="Usuario",
            family_name="Demo",
            is_active=True,
            last_login_at=datetime.now(timezone.utc),
        )
        auth_session = AuthSession(
            user_id=user_id,
            session_token_hash=hashlib.sha256(raw_token.encode("utf-8")).hexdigest(),
            csrf_token=csrf_token,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=4),
            last_seen_at=datetime.now(timezone.utc),
            ip_hash="ip",
            user_agent="pytest",
        )
        session.add(user)
        session.add(auth_session)
        await session.commit()
    return user_id, csrf_token


async def _clear_test_user_profile() -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(delete(AppUserProfile).where(AppUserProfile.user_id == "test-user"))
        await session.commit()


class AuthFlowTests(unittest.TestCase):
    def test_auth_me_returns_synthetic_user_in_testing(self):
        asyncio.run(_clear_test_user_profile())
        with TestClient(app) as client:
            response = client.get("/api/v1/auth/me")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["authenticated"])
        self.assertEqual(response.json()["user"]["email"], "test@agroclimax.local")
        self.assertIn("profile_status", response.json())
        self.assertFalse(response.json()["profile_status"]["is_complete"])

    def test_protected_endpoint_requires_login_without_bypass(self):
        with patch.object(settings, "auth_bypass_for_tests", False):
            with TestClient(app) as client:
                response = client.get("/api/v1/settings")
        self.assertEqual(response.status_code, 401)

    def test_google_login_redirects_to_google_authorize_url(self):
        discovery = {
            "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
        }
        with patch.object(settings, "google_client_id", "google-client-id"):
            with patch.object(settings, "google_client_secret", "google-client-secret"):
                with patch("app.services.auth.get_google_discovery_document", new=AsyncMock(return_value=discovery)):
                    with TestClient(app) as client:
                        response = client.get("/api/v1/auth/google/login?next=%2Fsettings", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn("accounts.google.com", response.headers["location"])
        self.assertIn("client_id=google-client-id", response.headers["location"])

    def test_mutating_route_requires_csrf_and_accepts_valid_token(self):
        payload = {"status": "ok", "recalculation_status": {"status": "completed"}}
        raw_token = f"raw-session-token-{uuid.uuid4()}"

        with patch.object(settings, "auth_bypass_for_tests", False):
            with TestClient(app) as client:
                _, csrf_token = asyncio.run(_seed_real_auth_session(raw_token))
                client.cookies.set(settings.auth_cookie_name, raw_token)

                response = client.put(
                    "/api/v1/settings/global",
                    json={"rules": {}},
                )
                self.assertEqual(response.status_code, 403)

                with patch("app.api.v1.endpoints.settings.save_global_settings", new=AsyncMock(return_value=payload)):
                    response = client.put(
                        "/api/v1/settings/global",
                        headers={settings.auth_csrf_header_name: csrf_token},
                        json={"rules": {}},
                    )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_profile_page_route_serves_html_shell(self):
        with TestClient(app) as client:
            response = client.get("/perfil")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Perfil de Usuario", response.text)
