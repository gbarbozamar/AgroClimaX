"""
AgroClimaX — Backend Principal
Sistema de Alertas Agroclimáticas por Teledetección
Rivera, Uruguay
"""
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.api.v1.router import api_router
from app.db.session import engine, Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("AgroClimaX iniciando — área: %s", settings.aoi_department)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    logger.info("AgroClimaX cerrando")
    await engine.dispose()


app = FastAPI(
    title="AgroClimaX API",
    description=(
        "Sistema de Alertas Agroclimáticas por Teledetección — Rivera, Uruguay. "
        "Integra Sentinel-1 (radar), Sentinel-2 (óptico) y ERA5 (clima) "
        "de Copernicus para detección temprana de estrés hídrico."
    ),
    version="0.1.0",
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


@app.get("/")
async def root():
    return {
        "sistema": "AgroClimaX",
        "descripcion": "Alertas Agroclimáticas por Teledetección — Rivera, Uruguay",
        "version": "0.1.0",
        "fuentes_datos": ["Sentinel-1 GRD", "Sentinel-2 L2A", "ERA5 CDS"],
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
