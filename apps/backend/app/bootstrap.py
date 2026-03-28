from __future__ import annotations

import logging

from sqlalchemy import text

from app.core.config import settings
from app.db.session import AsyncSessionLocal, Base, SPATIAL_BACKEND_ENABLED, SQLITE_BACKEND_ENABLED, engine
from app.services.analysis import ensure_latest_daily_analysis
from app.services.catalog import seed_catalog_units
from app.services.public_api import prewarm_coneat_tiles
from app.services.sections import seed_police_section_units
from app.services.warehouse import seed_layer_catalog


logger = logging.getLogger(__name__)


async def initialize_application_state() -> None:
    if SPATIAL_BACKEND_ENABLED and settings.database_use_postgis:
        async with engine.connect() as conn:
            try:
                autocommit_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
                await autocommit_conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            except Exception:
                logger.exception("No se pudo habilitar PostGIS; se sigue sin extension espacial")

    async with engine.begin() as conn:
        if SQLITE_BACKEND_ENABLED:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA synchronous=NORMAL"))
            await conn.execute(text("PRAGMA busy_timeout=30000"))
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        await seed_catalog_units(session)
        await seed_police_section_units(session)
        await seed_layer_catalog(session)
        await session.commit()


async def run_startup_warmup() -> None:
    async with AsyncSessionLocal() as session:
        result = await ensure_latest_daily_analysis(session)
        logger.info("Warmup diario listo: %s", result.get("status", "processed"))
    if settings.coneat_prewarm_enabled:
        prewarm = await prewarm_coneat_tiles()
        logger.info(
            "Precache CONEAT listo: %s tiles nuevas, %s reutilizadas",
            prewarm.get("warmed_tiles"),
            prewarm.get("reused_tiles"),
        )
