"""Script de dev: genera snapshots PNG mock para un campo dado.

Escribe 7 frames PNG sintéticos + filas FieldImageSnapshot en DB, una por
día (últimos 7 días). Sirve para probar Fase 3 (timeline) / Fase 4 (video
timelapse) / Fase 5 (MCP) sin tener que correr el pipeline real de
Copernicus.

Uso desde apps/backend:
    python scripts/seed_field_snapshots_mock.py <field_id> [layer_key] [n_days]
"""
from __future__ import annotations

import asyncio
import io
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

# Path hack para poder correr el script directamente.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PIL import Image, ImageDraw, ImageFont  # noqa: E402

from app.db.session import AsyncSessionLocal, Base, engine  # noqa: E402
from app.models.field_snapshot import FieldImageSnapshot  # noqa: E402


SNAPSHOT_ROOT = ROOT / ".tile_cache" / "fields"


def _make_mock_png(day_index: int, n_days: int, label: str) -> bytes:
    """PNG 512x512 con gradiente verde (simula NDVI cambiando en el tiempo)."""
    # Verde más claro hacia el final del período (simula "recuperación").
    green_intensity = int(80 + (day_index / max(1, n_days - 1)) * 140)
    img = Image.new("RGBA", (512, 512), (30, green_intensity, 60, 255))
    draw = ImageDraw.Draw(img)
    # Textos superpuestos para ver que el frame cambia.
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None
    draw.text((20, 20), label, fill=(255, 255, 255, 255), font=font)
    draw.text((20, 50), f"frame {day_index + 1}/{n_days}", fill=(255, 255, 255, 255), font=font)
    # Un pattern que varíe para que el video se vea animado.
    for i in range(0, 512, 32):
        x = (i + day_index * 30) % 512
        draw.ellipse((x - 5, 256 - 5, x + 5, 256 + 5), fill=(255, 200, 0, 200))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


async def seed(field_id: str, layer_key: str = "ndvi", n_days: int = 7) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    out_dir = SNAPSHOT_ROOT / field_id / "snapshots" / layer_key
    out_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    inserted = 0
    async with AsyncSessionLocal() as db:
        for i in range(n_days):
            observed = today - timedelta(days=(n_days - 1 - i))
            png_bytes = _make_mock_png(i, n_days, f"{layer_key} {observed.isoformat()}")
            out_path = out_dir / f"{observed.isoformat()}.png"
            out_path.write_bytes(png_bytes)

            # Upsert. Chequeamos existencia primero.
            from sqlalchemy import select
            existing = await db.execute(
                select(FieldImageSnapshot).where(
                    FieldImageSnapshot.field_id == field_id,
                    FieldImageSnapshot.layer_key == layer_key,
                    FieldImageSnapshot.observed_at == observed,
                )
            )
            row = existing.scalar_one_or_none()
            payload = {
                "field_id": field_id,
                "user_id": "test-user",
                "layer_key": layer_key,
                "observed_at": observed,
                "storage_key": f"fields/{field_id}/snapshots/{layer_key}/{observed.isoformat()}.png",
                "width_px": 512,
                "height_px": 512,
                "bbox_json": [-55.54, -31.08, -55.53, -31.07],
                "area_ha": 120.0 + i * 0.5,
                "risk_score": 45.0 - i * 2.5,
                "confidence_score": 70.0 + i * 0.8,
                "s1_humidity_mean_pct": 32.0 + i * 1.2,
                "s2_ndmi_mean": 0.22 + (i * 0.02),
                "spi_30d": -0.4 + (i * 0.05),
                "rendered_at": datetime.now(timezone.utc),
            }
            if row is None:
                row = FieldImageSnapshot(id=str(uuid4()), **payload)
                db.add(row)
            else:
                for k, v in payload.items():
                    if k in ("field_id", "layer_key", "observed_at"):
                        continue
                    setattr(row, k, v)
            inserted += 1
        await db.commit()

    print(f"[seed] {inserted} snapshots creados en DB + {inserted} PNGs en {out_dir}")
    print(f"[seed] field_id={field_id} layer={layer_key}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso: python scripts/seed_field_snapshots_mock.py <field_id> [layer] [n_days]")
        sys.exit(1)
    fid = sys.argv[1]
    layer = sys.argv[2] if len(sys.argv) > 2 else "ndvi"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 7
    # Env para que use la DB correcta (la misma que backend live).
    os.environ.setdefault("APP_ENV", "development")
    asyncio.run(seed(fid, layer, n))
