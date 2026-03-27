"""
AgroClimaX - Backend principal y runtime canonico.
"""
import asyncio
from contextlib import asynccontextmanager, suppress
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app.api.v1.router import api_router
from app.core.config import settings
from app.db.session import AsyncSessionLocal, Base, SPATIAL_BACKEND_ENABLED, SQLITE_BACKEND_ENABLED, engine
from app.models import *  # noqa: F401,F403
from app.services.analysis import ensure_latest_daily_analysis
from app.services.catalog import seed_catalog_units
from app.services.pipeline_ops import scheduler_loop, stop_scheduler
from app.services.public_api import prewarm_coneat_tiles
from app.services.sections import seed_police_section_units
from app.services.warehouse import seed_layer_catalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_ROOT = Path(__file__).resolve().parents[2] / "frontend"


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("AgroClimaX iniciando - entorno=%s", settings.app_env)
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

    async def _warmup_today() -> None:
        try:
            async with AsyncSessionLocal() as session:
                result = await ensure_latest_daily_analysis(session)
                logger.info("Warmup diario listo: %s", result.get("status", "processed"))
            if settings.coneat_prewarm_enabled:
                prewarm = await prewarm_coneat_tiles()
                logger.info("Precache CONEAT listo: %s tiles nuevas, %s reutilizadas", prewarm.get("warmed_tiles"), prewarm.get("reused_tiles"))
        except Exception:
            logger.exception("Fallo el warmup diario del pipeline")

    warmup_task = None
    scheduler_task = None
    if settings.pipeline_scheduler_enabled:
        scheduler_task = asyncio.create_task(scheduler_loop())
    elif settings.pipeline_startup_warmup_enabled:
        warmup_task = asyncio.create_task(_warmup_today())

    yield
    if warmup_task is not None and not warmup_task.done():
        warmup_task.cancel()
        with suppress(asyncio.CancelledError):
            await warmup_task
    await stop_scheduler(scheduler_task)
    logger.info("AgroClimaX cerrando")
    await engine.dispose()


app = FastAPI(
    title="AgroClimaX API",
    description=(
        "Sistema nacional de alertas agroclimaticas con calibracion automatica, "
        "score compuesto, confianza y trazabilidad."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.api_prefix)
app.include_router(api_router, prefix=settings.legacy_api_prefix)


@app.get("/health")
@app.get("/api/health")
async def health():
    return {"status": "ok", "runtime": "app.main", "version": "1.0.0"}


@app.get("/")
async def root():
    index_path = FRONTEND_ROOT / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"sistema": "AgroClimaX", "docs": "/docs"}


if FRONTEND_ROOT.exists():
    app.mount(settings.frontend_mount_path, StaticFiles(directory=str(FRONTEND_ROOT)), name="static")

    @app.get("/favicon.ico", include_in_schema=False)
    @app.get("/static/favicon.ico", include_in_schema=False)
    async def favicon():
        return FileResponse(FRONTEND_ROOT / "logo_agroclimax_header.png")

    @app.get("/logo.png", include_in_schema=False)
    @app.get("/static/logo.png", include_in_schema=False)
    async def logo():
        return FileResponse(FRONTEND_ROOT / "logo.png")

    @app.get("/AIDeepEconomics.png", include_in_schema=False)
    @app.get("/static/AIDeepEconomics.png", include_in_schema=False)
    async def ai_logo():
        return FileResponse(FRONTEND_ROOT / "AIDeepEconomics.png")
