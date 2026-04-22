"""Elimina FieldImageSnapshot rows cuyo PNG no existe o es inválido."""
from __future__ import annotations
import asyncio, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.field_snapshot import FieldImageSnapshot

MIN_SIZE = 1200

async def main():
    async with AsyncSessionLocal() as db:
        rows = (await db.execute(select(FieldImageSnapshot))).scalars().all()
        deleted = 0
        for r in rows:
            path = Path(".tile_cache") / (r.storage_key or "")
            bad = (not r.storage_key) or (not path.exists()) or path.stat().st_size < MIN_SIZE
            if bad:
                await db.delete(r)
                deleted += 1
        await db.commit()
    print(f"phantom rows deleted: {deleted}")

if __name__ == "__main__":
    asyncio.run(main())
