from importlib.util import find_spec
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings


def _resolve_database_url() -> str:
    if settings.database_url.startswith("postgresql+asyncpg") and find_spec("asyncpg") is None:
        fallback_db = Path(__file__).resolve().parents[2] / "agroclimax.db"
        return f"sqlite+aiosqlite:///{fallback_db.as_posix()}"
    return settings.database_url


engine = create_async_engine(_resolve_database_url(), echo=False, future=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
