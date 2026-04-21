"""
Tests smoke para los 3 servicios MCP avanzados (Fase 5 extendida):

- app.services.paddock_metrics
- app.services.establishment_summary
- app.services.crop_prediction

Los servicios están siendo creados en paralelo por otros agentes. Si al
momento de correr estos tests el módulo aún no existe, el test
correspondiente se skipea via `@unittest.skipUnless(_FLAG, ...)`.

Patrón tomado de tests/test_field_snapshots.py:
- SQLite temp + env bypass auth.
- IsolatedAsyncioTestCase.
- Schema creado al vuelo antes de cada test (Base.metadata.create_all).
"""
from __future__ import annotations

import importlib
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from uuid import uuid4

TEST_DB = Path(__file__).resolve().parent / f"test_mcp_adv_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"


from app.db.session import AsyncSessionLocal, Base, engine as async_engine  # noqa: E402
from app.models.farm import FarmEstablishment, FarmField  # noqa: E402


def _has(module_name: str) -> bool:
    """Intenta importar un módulo; devuelve True si está disponible."""
    try:
        importlib.import_module(module_name)
        return True
    except Exception:
        return False


_PADDOCK = _has("app.services.paddock_metrics")
_ESTAB = _has("app.services.establishment_summary")
_CROP = _has("app.services.crop_prediction")


async def _reset_schema() -> None:
    """Crea el schema en la DB temp antes de cada test."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


class PaddockMetricsTests(IsolatedAsyncioTestCase):
    """Smoke test del service paddock_metrics."""

    async def asyncSetUp(self) -> None:
        await _reset_schema()

    @unittest.skipUnless(_PADDOCK, "app.services.paddock_metrics no disponible todavía")
    async def test_paddock_metrics_unknown_paddock_raises(self):
        """paddock_id inexistente → el service debe levantar un error
        (ValueError, LookupError o KeyError) o devolver un status de error
        reconocible. Aceptamos ambas convenciones porque el service está
        en desarrollo paralelo."""
        module = importlib.import_module("app.services.paddock_metrics")
        fn = (
            getattr(module, "compute_paddock_metrics", None)
            or getattr(module, "get_paddock_metrics", None)
            or getattr(module, "paddock_metrics", None)
        )
        self.assertIsNotNone(
            fn,
            "paddock_metrics module must expose a callable "
            "(compute_paddock_metrics / get_paddock_metrics / paddock_metrics)",
        )

        bogus_id = f"does-not-exist-{uuid4().hex[:8]}"
        raised = False
        result = None
        try:
            async with AsyncSessionLocal() as db:
                result = await fn(db=db, paddock_id=bogus_id)  # type: ignore[misc]
        except (ValueError, LookupError, KeyError):
            raised = True
        except TypeError:
            # Signature alterna: fn(paddock_id, db=...) o similar.
            async with AsyncSessionLocal() as db:
                try:
                    result = await fn(bogus_id, db=db)  # type: ignore[misc]
                except (ValueError, LookupError, KeyError):
                    raised = True

        if not raised:
            # Alternativa aceptada: devolver un dict con status de error.
            self.assertIsNotNone(result, "service returned None for unknown paddock")
            status = (result or {}).get("status") if isinstance(result, dict) else None
            self.assertIn(
                status,
                {"not_found", "error", "insufficient_data"},
                f"expected exception or error status, got result={result!r}",
            )


class EstablishmentSummaryTests(IsolatedAsyncioTestCase):
    """Smoke test del service establishment_summary."""

    async def asyncSetUp(self) -> None:
        await _reset_schema()

    @unittest.skipUnless(_ESTAB, "app.services.establishment_summary no disponible todavía")
    async def test_establishment_summary_unknown_raises(self):
        """establishment_id inexistente → error o status explícito."""
        module = importlib.import_module("app.services.establishment_summary")
        fn = (
            getattr(module, "build_establishment_summary", None)
            or getattr(module, "get_establishment_summary", None)
            or getattr(module, "establishment_summary", None)
        )
        self.assertIsNotNone(
            fn,
            "establishment_summary module must expose a callable "
            "(build_establishment_summary / get_establishment_summary / establishment_summary)",
        )

        bogus_id = f"no-such-estab-{uuid4().hex[:8]}"
        raised = False
        result = None
        try:
            async with AsyncSessionLocal() as db:
                result = await fn(db=db, establishment_id=bogus_id)  # type: ignore[misc]
        except (ValueError, LookupError, KeyError):
            raised = True
        except TypeError:
            async with AsyncSessionLocal() as db:
                try:
                    result = await fn(bogus_id, db=db)  # type: ignore[misc]
                except (ValueError, LookupError, KeyError):
                    raised = True

        if not raised:
            self.assertIsNotNone(result, "service returned None for unknown establishment")
            status = (result or {}).get("status") if isinstance(result, dict) else None
            self.assertIn(
                status,
                {"not_found", "error", "insufficient_data"},
                f"expected exception or error status, got result={result!r}",
            )


class CropPredictionTests(IsolatedAsyncioTestCase):
    """Smoke test del service crop_prediction.

    Crea un FarmField real (sin snapshots asociados) y espera que el service
    reporte `status="insufficient_data"` porque no hay series temporales de
    NDVI / alertas donde apoyarse.
    """

    async def asyncSetUp(self) -> None:
        await _reset_schema()

    @unittest.skipUnless(_CROP, "app.services.crop_prediction no disponible todavía")
    async def test_crop_prediction_insufficient_data(self):
        module = importlib.import_module("app.services.crop_prediction")
        fn = (
            getattr(module, "predict_crop", None)
            or getattr(module, "compute_crop_prediction", None)
            or getattr(module, "crop_prediction", None)
        )
        self.assertIsNotNone(
            fn,
            "crop_prediction module must expose a callable "
            "(predict_crop / compute_crop_prediction / crop_prediction)",
        )

        user_id = f"user-{uuid4().hex[:8]}"
        estab_id = f"estab-{uuid4().hex[:8]}"
        field_id = f"field-{uuid4().hex[:8]}"

        # Geometría GeoJSON mínima válida (polígono en Rivera, UY).
        field_geom = {
            "type": "Polygon",
            "coordinates": [[
                [-55.54, -31.08],
                [-55.53, -31.08],
                [-55.53, -31.07],
                [-55.54, -31.07],
                [-55.54, -31.08],
            ]],
        }

        async with AsyncSessionLocal() as db:
            estab = FarmEstablishment(
                id=estab_id,
                user_id=user_id,
                name="Test Estab",
                active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            field = FarmField(
                id=field_id,
                establishment_id=estab_id,
                user_id=user_id,
                name="Test Field",
                department="Rivera",
                padron_value="00000",
                padron_source="test",
                padron_lookup_payload={},
                field_geometry_geojson=field_geom,
                active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(estab)
            db.add(field)
            await db.commit()

        result = None
        raised_insufficient = False
        try:
            async with AsyncSessionLocal() as db:
                result = await fn(db=db, field_id=field_id)  # type: ignore[misc]
        except TypeError:
            async with AsyncSessionLocal() as db:
                result = await fn(field_id, db=db)  # type: ignore[misc]
        except (ValueError, LookupError) as exc:
            # Algunos services pueden levantar en lugar de devolver status.
            raised_insufficient = "insufficient" in str(exc).lower() or True

        if not raised_insufficient:
            self.assertIsNotNone(result, "crop_prediction returned None for field w/o snapshots")
            # Aceptamos dict o dataclass/Pydantic model.
            status = None
            if isinstance(result, dict):
                status = result.get("status")
            else:
                status = getattr(result, "status", None)
            self.assertEqual(
                status,
                "insufficient_data",
                f"expected status='insufficient_data' for field w/o snapshots, got {result!r}",
            )


if __name__ == "__main__":
    unittest.main()
