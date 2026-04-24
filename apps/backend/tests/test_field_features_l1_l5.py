"""
Tests integrados L1-L5 para Field Mode (validación end-to-end).

Objetivo: verificar que los endpoints agregados por los agentes paralelos
conectan correctamente con la capa de auth + ownership + filesystem. Cada
test se skipea si el endpoint correspondiente todavía no fue mergeado (se
hace una introspección HEAD/GET para detectar 404 en el path).

L1: GET /api/v1/campos/{field_id}/layers-available
L2: POST /api/v1/campos/{field_id}/backfill-snapshots
L3: GET /api/v1/campos/{field_id}/snapshots/{storage_key:path} sirve PNG
L4: GET /api/v1/campos/{field_id}/timeline-frames shape incluye image_url absoluto
L5: layers-available usado por modal de video para poblar dropdown (shape contract)

Pattern tomado de tests/test_field_scope.py y test_integration_fase_2_5.py:
- SQLite temp DB por ejecución.
- AUTH_BYPASS_FOR_TESTS=true → auth inyecta user_id="test-user".
- Seeds mínimos: FarmField owned by test-user + FieldImageSnapshot rows.
"""
from __future__ import annotations

import asyncio
import io
import os
from datetime import date
from pathlib import Path
import unittest
from unittest import IsolatedAsyncioTestCase
from uuid import uuid4

import pytest

TEST_DB = Path(__file__).resolve().parent / f"test_field_features_l1_l5_{uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
os.environ["APP_RUNTIME_ROLE"] = "web"
os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
os.environ["CONEAT_PREWARM_ENABLED"] = "false"
os.environ["DATABASE_USE_POSTGIS"] = "false"
os.environ.setdefault("MCP_SERVICE_TOKEN", "dev-test-token")


# ---------------------------------------------------------------------------
# Import checks — si el endpoint todavía no existe, skipeamos el test.
# ---------------------------------------------------------------------------
def _has_module(path: str) -> bool:
    try:
        __import__(path)
        return True
    except Exception:
        return False


def _route_exists(path_suffix: str) -> bool:
    """Valida que alguna ruta registrada en field_timeline termine con path_suffix."""
    if not _has_module("app.api.v1.endpoints.field_timeline"):
        return False
    from app.api.v1.endpoints import field_timeline  # type: ignore
    for r in getattr(field_timeline, "router").routes:
        if path_suffix in getattr(r, "path", ""):
            return True
    return False


_HAS_L1 = _route_exists("layers-available")
_HAS_L2 = _route_exists("backfill-snapshots")
_HAS_L3 = _route_exists("snapshots/{storage_key")  # FastAPI stores path converter inline
_HAS_L4 = _route_exists("timeline-frames")
_HAS_L5 = _HAS_L1  # modal depende del mismo endpoint L1 para poblar dropdown


# ---------------------------------------------------------------------------
# Helpers de seeding.
# ---------------------------------------------------------------------------
def _make_fake_png(color: tuple[int, int, int, int] = (50, 150, 50, 255)) -> bytes:
    """PNG 256x256 opaco válido (≥ 500b para evitar falsos transparentes)."""
    from PIL import Image
    img = Image.new("RGBA", (256, 256), color)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


async def _reset_schema() -> None:
    from app.db.session import Base, engine as async_engine  # noqa: WPS433
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _seed_field_with_snapshots(
    user_id: str = "test-user",
    layers: tuple[str, ...] = ("ndvi", "ndmi", "alerta_fusion"),
    write_png_on_disk: bool = True,
) -> tuple[str, str, list[str]]:
    """Crea un FarmField + FieldImageSnapshot por cada layer. Devuelve (field_id, first_storage_key, all_storage_keys).

    Si `write_png_on_disk`, escribe el archivo PNG en .tile_cache/fields/{field_id}/...
    para que el endpoint de snapshots pueda servirlo desde FileResponse.
    """
    from app.db.session import AsyncSessionLocal
    from app.models.farm import FarmField
    from app.models.field_snapshot import FieldImageSnapshot

    field_id = f"field-l15-{uuid4().hex[:10]}"
    storage_keys: list[str] = []

    tile_root = Path(".tile_cache")
    field_dir = tile_root / "fields" / field_id
    if write_png_on_disk:
        field_dir.mkdir(parents=True, exist_ok=True)

    async with AsyncSessionLocal() as db:
        db.add(FarmField(
            id=field_id,
            user_id=user_id,
            establishment_id=f"est-{uuid4().hex[:8]}",
            name="Campo L1-L5 Test",
            department="Rivera",
            padron_value="test-12345",
            field_geometry_geojson={
                "type": "Polygon",
                "coordinates": [[[-55.54, -31.08], [-55.53, -31.08],
                                 [-55.53, -31.07], [-55.54, -31.07],
                                 [-55.54, -31.08]]],
            },
            active=True,
        ))
        for idx, layer in enumerate(layers):
            fname = f"{layer}_2026-04-2{idx}.png"
            storage_key = f"fields/{field_id}/{fname}"
            storage_keys.append(storage_key)
            if write_png_on_disk:
                (field_dir / fname).write_bytes(_make_fake_png())
            db.add(FieldImageSnapshot(
                field_id=field_id,
                user_id=user_id,
                layer_key=layer,
                observed_at=date(2026, 4, 20 + idx),
                storage_key=storage_key,
                width_px=256,
                height_px=256,
                bbox_json=[-55.54, -31.08, -55.53, -31.07],
                area_ha=10.0,
            ))
        await db.commit()

    return field_id, storage_keys[0], storage_keys


# ---------------------------------------------------------------------------
# Tests L1-L5
# ---------------------------------------------------------------------------
class FieldFeaturesL1L5Tests(IsolatedAsyncioTestCase):
    """Validación integrada de los endpoints agregados por agentes paralelos."""

    async def asyncSetUp(self) -> None:
        await _reset_schema()

    @pytest.mark.skipif(not _HAS_L1, reason="L1 (layers-available endpoint) pendiente")
    async def test_01_layers_available_returns_list(self):
        """L1 — GET /campos/{id}/layers-available → 200 con array de capas."""
        from fastapi.testclient import TestClient

        from app.main import app

        field_id, _, _ = await _seed_field_with_snapshots(user_id="test-user")
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/campos/{field_id}/layers-available")

        self.assertEqual(resp.status_code, 200, resp.text)
        data = resp.json()
        self.assertEqual(data["field_id"], field_id)
        self.assertIsInstance(data["layers"], list)
        keys = {entry["layer_key"] for entry in data["layers"]}
        # Al menos las capas que seedeamos deben estar (ndvi, ndmi, alerta_fusion).
        for expected in ("ndvi", "ndmi", "alerta_fusion"):
            self.assertIn(expected, keys, f"layer {expected!r} ausente en layers-available")
        # Shape per-layer: layer_key, count, first_observed, last_observed, label.
        for entry in data["layers"]:
            self.assertIn("count", entry)
            self.assertIn("first_observed", entry)
            self.assertIn("last_observed", entry)
            self.assertIn("label", entry)

    @pytest.mark.skipif(not _HAS_L2, reason="L2 (backfill-snapshots endpoint) pendiente")
    async def test_02_backfill_snapshots_returns_scheduled(self):
        """L2 — POST /campos/{id}/backfill-snapshots → 202 status=scheduled."""
        from fastapi.testclient import TestClient

        from app.main import app

        field_id, _, _ = await _seed_field_with_snapshots(
            user_id="test-user", write_png_on_disk=False,
        )
        with TestClient(app) as client:
            resp = client.post(
                f"/api/v1/campos/{field_id}/backfill-snapshots",
                json={"days": 7, "layers": ["ndvi"]},
            )

        # El contrato dice 202 (background task scheduled) o 200 (procesado inline).
        self.assertIn(resp.status_code, (200, 202), resp.text)
        data = resp.json()
        self.assertEqual(data.get("status"), "scheduled")
        self.assertEqual(data.get("field_id"), field_id)
        self.assertEqual(data.get("days"), 7)
        self.assertEqual(data.get("layers"), ["ndvi"])

    @pytest.mark.skipif(
        not (_HAS_L3 and _HAS_L4),
        reason="L3/L4 (snapshots image + timeline-frames) pendientes",
    )
    async def test_03_snapshot_image_served_correctly(self):
        """L3 — el image_url de timeline-frames resuelve a un PNG válido (>500b)."""
        from fastapi.testclient import TestClient

        from app.main import app

        field_id, _, _ = await _seed_field_with_snapshots(
            user_id="test-user",
            layers=("ndvi",),
            write_png_on_disk=True,
        )
        with TestClient(app) as client:
            frames_resp = client.get(
                f"/api/v1/campos/{field_id}/timeline-frames",
                params={"layer": "ndvi", "days": 30},
            )
            self.assertEqual(frames_resp.status_code, 200, frames_resp.text)
            frames = frames_resp.json()["days"]
            self.assertGreater(len(frames), 0, "timeline-frames devolvió lista vacía")

            first_url = frames[0]["image_url"]
            # image_url debe ser absoluto (path-absolute) al backend.
            self.assertTrue(
                first_url.startswith(f"/api/v1/campos/{field_id}/snapshots/fields/{field_id}/"),
                f"image_url inesperado: {first_url}",
            )
            img_resp = client.get(first_url)

        self.assertEqual(img_resp.status_code, 200, img_resp.text)
        self.assertEqual(img_resp.headers.get("content-type"), "image/png")
        self.assertGreater(len(img_resp.content), 500, "PNG sospechosamente chico (<500b)")
        # PNG signature check.
        self.assertEqual(img_resp.content[:8], b"\x89PNG\r\n\x1a\n")

    @pytest.mark.skipif(not _HAS_L4, reason="L4 (timeline-frames endpoint) pendiente")
    async def test_04_timeline_frames_urls_match_endpoint(self):
        """L4 — image_url empieza con /api/v1/campos/{id}/snapshots/fields/{id}/."""
        from fastapi.testclient import TestClient

        from app.main import app

        field_id, _, _ = await _seed_field_with_snapshots(
            user_id="test-user",
            layers=("ndvi",),
            write_png_on_disk=False,  # sólo validamos shape, no servimos la imagen
        )
        with TestClient(app) as client:
            resp = client.get(
                f"/api/v1/campos/{field_id}/timeline-frames",
                params={"layer": "ndvi", "days": 30},
            )

        self.assertEqual(resp.status_code, 200, resp.text)
        data = resp.json()
        self.assertEqual(data["field_id"], field_id)
        self.assertEqual(data["layer_key"], "ndvi")
        self.assertIn("days", data)
        self.assertGreater(len(data["days"]), 0)

        expected_prefix = f"/api/v1/campos/{field_id}/snapshots/fields/{field_id}/"
        for frame in data["days"]:
            self.assertIn("image_url", frame)
            self.assertIn("observed_at", frame)
            self.assertIn("metadata", frame)
            self.assertTrue(
                frame["image_url"].startswith(expected_prefix),
                f"image_url no matchea prefix esperado: {frame['image_url']!r}",
            )

    @pytest.mark.skipif(not _HAS_L5, reason="L5 (layers-available para modal) pendiente")
    async def test_05_layers_available_shape_for_video_modal(self):
        """L5 — layers-available expone label legible + ventana temporal para dropdown UI."""
        from fastapi.testclient import TestClient

        from app.main import app

        field_id, _, _ = await _seed_field_with_snapshots(
            user_id="test-user",
            layers=("ndvi", "ndmi", "alerta_fusion"),
            write_png_on_disk=False,
        )
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/campos/{field_id}/layers-available")

        self.assertEqual(resp.status_code, 200, resp.text)
        data = resp.json()
        # Shape contract del modal de video (L5):
        # - cada layer debe exponer un label humano (no layer_key uppercase por defecto).
        # - first_observed y last_observed son strings ISO-date (o None).
        # - count > 0.
        ndvi = next((l for l in data["layers"] if l["layer_key"] == "ndvi"), None)
        self.assertIsNotNone(ndvi, "ndvi no aparece en layers-available")
        self.assertIsInstance(ndvi["label"], str)
        self.assertGreater(len(ndvi["label"]), 0)
        self.assertGreaterEqual(ndvi["count"], 1)
        # Las fechas son ISO (YYYY-MM-DD) o None.
        for fld in ("first_observed", "last_observed"):
            val = ndvi[fld]
            self.assertTrue(val is None or (isinstance(val, str) and len(val) == 10))


if __name__ == "__main__":
    unittest.main()
