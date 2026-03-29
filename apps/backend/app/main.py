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
from starlette.middleware.sessions import SessionMiddleware

from app.api.v1.router import api_router
from app.bootstrap import initialize_application_state, run_startup_warmup
from app.core.config import settings
from app.db.session import engine
from app.models import *  # noqa: F401,F403
from app.services.pipeline_ops import scheduler_loop, stop_scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_ROOT = Path(__file__).resolve().parents[2] / "frontend"


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("AgroClimaX iniciando - entorno=%s role=%s", settings.app_env, settings.app_runtime_role)
    await initialize_application_state()

    warmup_task = None
    scheduler_task = None
    if settings.app_runtime_role == "all-in-one" and settings.pipeline_scheduler_enabled:
        scheduler_task = asyncio.create_task(scheduler_loop())
    elif settings.app_runtime_role == "all-in-one" and settings.pipeline_startup_warmup_enabled:
        async def _warmup_today() -> None:
            try:
                await run_startup_warmup()
            except Exception:
                logger.exception("Fallo el warmup diario del pipeline")

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
    SessionMiddleware,
    secret_key=settings.secret_key,
    session_cookie="agroclimax_oauth",
    same_site="lax",
    https_only=settings.app_env == "production",
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


@app.get("/perfil")
@app.get("/perfil/")
async def profile_page():
    profile_path = FRONTEND_ROOT / "profile.html"
    if profile_path.exists():
        return FileResponse(profile_path)
    return FileResponse(FRONTEND_ROOT / "index.html")


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
