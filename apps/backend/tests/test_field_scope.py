"""
Tests de Fase 1: field scope desbloqueado con auth + ownership.
"""
from __future__ import annotations

import os
from pathlib import Path
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from uuid import uuid4

TEST_DB = Path(__file__).resolve().parent / f"test_field_scope_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"

from fastapi.testclient import TestClient

from app.main import app
from app.services import aoi_tile_clip


class FieldScopeGeojsonTests(IsolatedAsyncioTestCase):
    """
    El endpoint público /geojson/{scope}/{ref} NO acepta scope=field.
    El endpoint protegido /geojson/field/{id} delega a resolve_scope_geometry
    pasando user_id del contexto auth.
    """

    def test_field_id_unknown_returns_404(self):
        """El endpoint protegido /geojson/field/{id} matchea primero y devuelve 404 si no existe."""
        with TestClient(app) as client:
            resp = client.get("/api/v1/geojson/field/any-id")
            self.assertEqual(resp.status_code, 404)

    def test_public_scope_unsupported_returns_400(self):
        with TestClient(app) as client:
            resp = client.get("/api/v1/geojson/foobar/xyz")
            self.assertEqual(resp.status_code, 400)

    def test_protected_field_passes_user_id_to_resolver(self):
        captured = {}

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            captured["scope"] = scope
            captured["ref"] = ref
            captured["user_id"] = user_id
            raise aoi_tile_clip.ScopeNotFoundError("stub")

        with patch(
            "app.api.v1.endpoints.geo_scopes.aoi_tile_clip.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            with TestClient(app) as client:
                resp = client.get("/api/v1/geojson/field/abc-123")
                self.assertEqual(resp.status_code, 404)
                self.assertEqual(captured["scope"], "field")
                self.assertEqual(captured["ref"], "abc-123")
                # El bypass de testing inyecta user_id=test-user
                self.assertEqual(captured["user_id"], "test-user")

    def test_protected_field_auth_error_returns_403(self):
        async def _fake_resolve(db, scope, ref, *, user_id=None):
            raise aoi_tile_clip.ScopeAuthError("user no own")

        with patch(
            "app.api.v1.endpoints.geo_scopes.aoi_tile_clip.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            with TestClient(app) as client:
                resp = client.get("/api/v1/geojson/field/someones-else")
                self.assertEqual(resp.status_code, 403)


class TilesClipScopeFieldTests(IsolatedAsyncioTestCase):
    """/tiles/... con clip_scope=field propaga user_id cuando auth existe."""

    def test_field_clip_with_bypass_propagates_user_id(self):
        captured = {}

        async def _fake_fetch(*args, **kwargs):
            captured.update(kwargs)
            from app.services.public_api import TRANSPARENT_PNG
            return TRANSPARENT_PNG

        with patch("app.api.v1.endpoints.public.fetch_tile_png", side_effect=_fake_fetch):
            with TestClient(app) as client:
                resp = client.get(
                    "/api/v1/tiles/alerta_fusion/7/44/76.png",
                    params={"clip_scope": "field", "clip_ref": "any-field"},
                )
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(captured.get("clip_scope"), "field")
                self.assertEqual(captured.get("clip_ref"), "any-field")
                # En testing bypass, siempre inyecta test-user
                self.assertEqual(captured.get("user_id"), "test-user")

    def test_field_clip_without_auth_degrades_to_none(self):
        """Sin sesión (tiles pedidos por <img> sin cookies), no devolver 401.

        Fallback degradado: dejar clip_scope=None para que el tile se sirva
        sin recorte server-side; el visual clipMask del frontend se encarga
        de ocultar el área exterior al potrero. Evita que el mapa quede
        completamente en blanco cuando Leaflet pide tiles sin cookies.
        """
        from app.core.config import settings as app_settings

        captured = {}

        async def _fake_fetch(*args, **kwargs):
            captured.update(kwargs)
            from app.services.public_api import TRANSPARENT_PNG
            return TRANSPARENT_PNG

        with patch.object(app_settings, "auth_bypass_for_tests", False), \
             patch("app.api.v1.endpoints.public.fetch_tile_png", side_effect=_fake_fetch):
            with TestClient(app) as client:
                resp = client.get(
                    "/api/v1/tiles/ndvi/15/11328/19361.png",
                    params={"clip_scope": "field", "clip_ref": "any-field"},
                )
                self.assertEqual(resp.status_code, 200)
                # Degradado: clip_scope/ref quedan None cuando no hay auth
                self.assertIsNone(captured.get("clip_scope"))
                self.assertIsNone(captured.get("clip_ref"))
                self.assertIsNone(captured.get("user_id"))


if __name__ == "__main__":
    unittest.main()
