"""Backfill de snapshots para todos los FarmField activos, N días hacia atrás.

Llama render_field_snapshot directamente (no pasa por el pipeline completo
de los 19 departamentos, que tarda 10+ min por día). Usa los tiles
Copernicus reales con clip_scope=field para cada (campo, capa, fecha).

Uso:
    python scripts/backfill_field_snapshots.py [n_days=14] [layers=ndvi,ndmi,rgb,alerta_fusion]

Tarda ~5-30s por snapshot (depende de cuántos tiles Copernicus baja).
Para 1 campo × 14 días × 4 layers = 56 snapshots ≈ 5-20 min total.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import select  # noqa: E402

from app.db.session import AsyncSessionLocal  # noqa: E402
from app.models.farm import FarmField  # noqa: E402
from app.services.field_snapshots import render_field_snapshot  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill")


async def backfill(n_days: int, layers: list[str]) -> None:
    async with AsyncSessionLocal() as db:
        fields = (await db.execute(select(FarmField).where(FarmField.active == True))).scalars().all()  # noqa: E712
    if not fields:
        logger.warning("No active FarmField rows found; nothing to backfill.")
        return

    today = date.today()
    total_ok = 0
    total_skip = 0
    total_err = 0
    logger.info(
        "Backfill starting: fields=%d days=%d layers=%s",
        len(fields), n_days, layers,
    )
    t0 = time.time()

    for field in fields:
        for i in range(n_days):
            target = today - timedelta(days=i)
            for layer in layers:
                label = f"field={field.id[:8]} layer={layer} date={target.isoformat()}"
                async with AsyncSessionLocal() as db:
                    try:
                        start = time.time()
                        snap = await render_field_snapshot(db, field.id, layer, target)
                        elapsed = time.time() - start
                        if snap is None:
                            total_skip += 1
                            logger.info("  SKIP %s (%.1fs) — no tiles", label, elapsed)
                        else:
                            total_ok += 1
                            await db.commit()
                            logger.info("  OK   %s (%.1fs)", label, elapsed)
                    except Exception as exc:
                        total_err += 1
                        logger.warning("  ERR  %s — %s", label, exc)
                        try:
                            await db.rollback()
                        except Exception:
                            pass

    elapsed = time.time() - t0
    logger.info(
        "Backfill done: ok=%d skip=%d err=%d total_time=%.1fs",
        total_ok, total_skip, total_err, elapsed,
    )


if __name__ == "__main__":
    n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 14
    layers_arg = sys.argv[2] if len(sys.argv) > 2 else "ndvi,ndmi,rgb,alerta_fusion"
    layers = [x.strip() for x in layers_arg.split(",") if x.strip()]
    os.environ.setdefault("APP_ENV", "development")
    asyncio.run(backfill(n_days, layers))
