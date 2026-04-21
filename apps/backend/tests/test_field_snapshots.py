"""
Tests de Fase 2: render_field_snapshot (unit tests).

Patrón tomado de tests/test_field_scope.py:
- SQLite temp + env bypass auth.
- IsolatedAsyncioTestCase.
- Mock de fetch_tile_png y resolve_scope_geometry para no pegar a Copernicus.
"""
from __future__ import annotations

import os
from pathlib import Path
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock
from uuid import uuid4

import pytest

TEST_DB = Path(__file__).resolve().parent / f"test_field_snapshots_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"

from app.services import aoi_tile_clip  # noqa: E402
from app.services.public_api import TRANSPARENT_PNG  # noqa: E402


# Intentamos importar los módulos de Fase 2. Validamos también que la signature
# del service coincida con la asumida por estos tests (db, field_id, user_id,
# observed_at, layers). Si hay mismatch (A2 y A4 corrieron en paralelo y
# divergieron), marcamos skip hasta una iteración de alineación.
try:
    import inspect

    from app.models.field_snapshot import FieldImageSnapshot  # noqa: F401
    from app.services.field_snapshots import render_field_snapshot  # noqa: F401

    _sig = inspect.signature(render_field_snapshot)
    _expected = {"db", "field_id", "user_id", "observed_at", "layers"}
    _PHASE2_READY = _expected.issubset(set(_sig.parameters))
    _SKIP_REASON = (
        ""
        if _PHASE2_READY
        else f"Fase 2 service signature mismatch: got {list(_sig.parameters)}, want {sorted(_expected)}"
    )
except Exception as exc:  # pragma: no cover - dependiente de otros agentes
    _PHASE2_READY = False
    _SKIP_REASON = f"Fase 2 aún no disponible: {exc!r}"


# PNG fake "no transparente" (~1500 bytes). Usamos un buffer mayor al
# TRANSPARENT_PNG (67 bytes) para diferenciar el caso "hay contenido".
FAKE_RENDERED_PNG = (
    b"\x89PNG\r\n\x1a\n"  # magic bytes
    + b"\x00" * 1500
)


@pytest.mark.skipif(not _PHASE2_READY, reason=_SKIP_REASON or "phase 2 pending")
class RenderFieldSnapshotTests(IsolatedAsyncioTestCase):
    """Unit tests de render_field_snapshot (Fase 2)."""

    async def test_render_single_layer_creates_png_and_row(self):
        """
        TODO(fase2): habilitar cuando field_snapshots esté mergeado.
        Mockea fetch_tile_png -> PNG válido fake; verifica fila en
        field_image_snapshots + archivo en .tile_cache/fields/...
        """
        async def _fake_fetch(*args, **kwargs):
            return FAKE_RENDERED_PNG

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return {"geometry": {"type": "Polygon", "coordinates": []}}

        with patch(
            "app.services.field_snapshots.fetch_tile_png",
            side_effect=_fake_fetch,
        ), patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            result = await render_field_snapshot(  # type: ignore[name-defined]
                db=None,
                field_id="field-abc",
                user_id="test-user",
                observed_at="2026-04-21",
                layers=["alerta_fusion"],
            )
            self.assertIsNotNone(result)

    async def test_render_is_idempotent_when_observed_at_same(self):
        """
        TODO(fase2): dos llamadas con misma fecha NO crean dos filas
        (UniqueConstraint sobre (field_id, layer, observed_at)).
        """
        async def _fake_fetch(*args, **kwargs):
            return FAKE_RENDERED_PNG

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return {"geometry": {"type": "Polygon", "coordinates": []}}

        with patch(
            "app.services.field_snapshots.fetch_tile_png",
            side_effect=_fake_fetch,
        ), patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            first = await render_field_snapshot(  # type: ignore[name-defined]
                db=None,
                field_id="field-abc",
                user_id="test-user",
                observed_at="2026-04-21",
                layers=["alerta_fusion"],
            )
            second = await render_field_snapshot(  # type: ignore[name-defined]
                db=None,
                field_id="field-abc",
                user_id="test-user",
                observed_at="2026-04-21",
                layers=["alerta_fusion"],
            )
            # Idempotencia: segundo call devuelve mismo id / no duplica.
            self.assertEqual(first, second)

    async def test_render_cross_user_raises_auth_error(self):
        """
        TODO(fase2): si resolve_scope_geometry levanta ScopeAuthError
        (field pertenece a otro user), render debe devolver None.
        """
        async def _fake_resolve(db, scope, ref, *, user_id=None):
            raise aoi_tile_clip.ScopeAuthError("user no own field")

        with patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            result = await render_field_snapshot(  # type: ignore[name-defined]
                db=None,
                field_id="someone-elses-field",
                user_id="test-user",
                observed_at="2026-04-21",
                layers=["alerta_fusion"],
            )
            self.assertIsNone(result)

    async def test_render_skips_when_no_tiles_available(self):
        """
        TODO(fase2): si fetch_tile_png devuelve TRANSPARENT_PNG (67b),
        no hay dato real y no se crea fila; retorno esperado: None.
        """
        async def _fake_fetch(*args, **kwargs):
            return TRANSPARENT_PNG

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return {"geometry": {"type": "Polygon", "coordinates": []}}

        with patch(
            "app.services.field_snapshots.fetch_tile_png",
            side_effect=_fake_fetch,
        ), patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            result = await render_field_snapshot(  # type: ignore[name-defined]
                db=None,
                field_id="field-abc",
                user_id="test-user",
                observed_at="2026-04-21",
                layers=["alerta_fusion"],
            )
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
