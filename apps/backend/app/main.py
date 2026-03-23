"""
AgroClimaX - Backend principal y runtime canonico.
"""
from contextlib import asynccontextmanager
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.v1.router import api_router
from app.core.config import settings
from app.db.session import AsyncSessionLocal, Base, engine
from app.models import *  # noqa: F401,F403
from app.services.catalog import seed_catalog_units

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_ROOT = Path(__file__).resolve().parents[2] / "frontend"


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("AgroClimaX iniciando - entorno=%s", settings.app_env)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        await seed_catalog_units(session)

    yield
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

    @app.get("/logo.png", include_in_schema=False)
    @app.get("/static/logo.png", include_in_schema=False)
    async def logo():
        return FileResponse(FRONTEND_ROOT / "logo.png")

    @app.get("/AIDeepEconomics.png", include_in_schema=False)
    @app.get("/static/AIDeepEconomics.png", include_in_schema=False)
    async def ai_logo():
        return FileResponse(FRONTEND_ROOT / "AIDeepEconomics.png")
