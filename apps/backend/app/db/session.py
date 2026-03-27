import logging
import socket
from importlib.util import find_spec
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings


logger = logging.getLogger(__name__)
LOCAL_DATABASE_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _sqlite_fallback_url() -> str:
    fallback_db = Path(__file__).resolve().parents[2] / "agroclimax.db"
    return f"sqlite+aiosqlite:///{fallback_db.as_posix()}"


def _local_database_is_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.6):
            return True
    except OSError:
        return False


def _resolve_database_url() -> str:
    configured_url = settings.database_url
    if configured_url.startswith("postgresql+asyncpg") and find_spec("asyncpg") is None:
        logger.warning("asyncpg no esta instalado; usando fallback SQLite local para desarrollo")
        return _sqlite_fallback_url()

    parsed = make_url(configured_url)
    if (
        settings.app_env != "production"
        and parsed.get_backend_name() == "postgresql"
        and (parsed.host or "localhost") in LOCAL_DATABASE_HOSTS
        and not _local_database_is_reachable(parsed.host or "localhost", parsed.port or 5432)
    ):
        logger.warning(
            "PostgreSQL local no disponible en %s:%s; usando fallback SQLite local para desarrollo",
            parsed.host or "localhost",
            parsed.port or 5432,
        )
        return _sqlite_fallback_url()

    return configured_url


RESOLVED_DATABASE_URL = _resolve_database_url()
DATABASE_BACKEND_NAME = make_url(RESOLVED_DATABASE_URL).get_backend_name()
SPATIAL_BACKEND_ENABLED = DATABASE_BACKEND_NAME == "postgresql"
SQLITE_BACKEND_ENABLED = DATABASE_BACKEND_NAME == "sqlite"

engine_kwargs = {"echo": False, "future": True}
if SQLITE_BACKEND_ENABLED:
    engine_kwargs["connect_args"] = {"timeout": 30}

engine = create_async_engine(RESOLVED_DATABASE_URL, **engine_kwargs)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
