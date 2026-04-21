"""
Tests de Fase 2: render_field_snapshot (unit tests).

Patrón tomado de tests/test_field_scope.py:
- SQLite temp + env bypass auth.
- IsolatedAsyncioTestCase.
- Mock de fetch_tile_png y resolve_scope_geometry para no pegar a Copernicus.

La signature real del service es:
    render_field_snapshot(db, field_id, layer_key, observed_at)
donde db es AsyncSession y observed_at es un datetime.date.
"""
from __future__ import annotations

import io
import os
from datetime import date
from pathlib import Path
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from uuid import uuid4

TEST_DB = Path(__file__).resolve().parent / f"test_field_snapshots_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"

from shapely.geometry import Polygon  # noqa: E402

from app.db.session import AsyncSessionLocal, Base, engine as async_engine  # noqa: E402
from app.models.field_snapshot import FieldImageSnapshot  # noqa: E402
from app.services import aoi_tile_clip  # noqa: E402
from app.services.field_snapshots import render_field_snapshot  # noqa: E402
from app.services.public_api import TRANSPARENT_PNG  # noqa: E402


def _make_fake_png() -> bytes:
    """PNG 256x256 opaco válido (≥ 500b para pasar el _is_valid_tile_png check)."""
    from PIL import Image
    img = Image.new("RGBA", (256, 256), (50, 150, 50, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# Pequeño rectángulo en Rivera, Uruguay (no colinda con borde del país).
TEST_FIELD_GEOM = Polygon([
    (-55.54, -31.08),
    (-55.53, -31.08),
    (-55.53, -31.07),
    (-55.54, -31.07),
    (-55.54, -31.08),
])

FAKE_PNG_BYTES = _make_fake_png()


async def _reset_schema() -> None:
    """Crea el schema en la DB temp antes de cada test."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


class RenderFieldSnapshotTests(IsolatedAsyncioTestCase):
    """Unit tests de render_field_snapshot (Fase 2)."""

    async def asyncSetUp(self) -> None:
        await _reset_schema()

    async def test_render_single_layer_creates_png_and_row(self):
        """PNG válido en cada tile → fila en field_image_snapshots + archivo en disco."""
        async def _fake_fetch(*args, **kwargs):
            return FAKE_PNG_BYTES

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return TEST_FIELD_GEOM

        field_id = f"field-{uuid4().hex[:8]}"
        with patch(
            "app.services.field_snapshots.fetch_tile_png",
            side_effect=_fake_fetch,
        ), patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            async with AsyncSessionLocal() as db:
                result = await render_field_snapshot(
                    db=db,
                    field_id=field_id,
                    layer_key="alerta_fusion",
                    observed_at=date(2026, 4, 21),
                )
                await db.commit()

            self.assertIsNotNone(result)
            self.assertEqual(result.field_id, field_id)
            self.assertEqual(result.layer_key, "alerta_fusion")
            # El service guarda el path absoluto del PNG en storage_path
            # (campo propio) o en storage_key según schema. Verificamos que
            # al menos uno esté seteado y apunte a un archivo existente.
            stored_path = getattr(result, "storage_path", None) or getattr(result, "storage_key", None)
            self.assertIsNotNone(stored_path, "expected storage_path o storage_key")
            self.assertTrue(Path(str(stored_path)).exists(), f"PNG no escrito en {stored_path}")

    async def test_render_is_idempotent_when_observed_at_same(self):
        """Dos renders con misma (field, layer, fecha) → una sola fila (upsert)."""
        async def _fake_fetch(*args, **kwargs):
            return FAKE_PNG_BYTES

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return TEST_FIELD_GEOM

        field_id = f"field-{uuid4().hex[:8]}"
        with patch(
            "app.services.field_snapshots.fetch_tile_png",
            side_effect=_fake_fetch,
        ), patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            async with AsyncSessionLocal() as db:
                first = await render_field_snapshot(
                    db=db,
                    field_id=field_id,
                    layer_key="ndvi",
                    observed_at=date(2026, 4, 21),
                )
                await db.commit()
                second = await render_field_snapshot(
                    db=db,
                    field_id=field_id,
                    layer_key="ndvi",
                    observed_at=date(2026, 4, 21),
                )
                await db.commit()

            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            # Idempotencia: misma PK aunque se haya re-renderizado.
            self.assertEqual(first.id, second.id)

    async def test_render_cross_user_raises_auth_error(self):
        """resolve_scope_geometry levanta ScopeAuthError → render devuelve None sin crashear."""
        async def _fake_resolve(db, scope, ref, *, user_id=None):
            raise aoi_tile_clip.ScopeAuthError("user does not own this field")

        with patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            async with AsyncSessionLocal() as db:
                result = await render_field_snapshot(
                    db=db,
                    field_id="someone-elses-field",
                    layer_key="alerta_fusion",
                    observed_at=date(2026, 4, 21),
                )
            self.assertIsNone(result)

    async def test_render_skips_when_no_tiles_available(self):
        """Todos los fetch_tile_png devuelven transparente (67b) → render returns None."""
        async def _fake_fetch(*args, **kwargs):
            return TRANSPARENT_PNG

        async def _fake_resolve(db, scope, ref, *, user_id=None):
            return TEST_FIELD_GEOM

        with patch(
            "app.services.field_snapshots.fetch_tile_png",
            side_effect=_fake_fetch,
        ), patch(
            "app.services.field_snapshots.resolve_scope_geometry",
            side_effect=_fake_resolve,
        ):
            async with AsyncSessionLocal() as db:
                result = await render_field_snapshot(
                    db=db,
                    field_id=f"field-{uuid4().hex[:8]}",
                    layer_key="alerta_fusion",
                    observed_at=date(2026, 4, 21),
                )
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
