from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.analysis import analyze_custom_geojson, get_legacy_history, get_legacy_state

router = APIRouter(tags=["legacy"])


@router.get("/estado-actual")
async def legacy_estado_actual(
    department: str = Query("Rivera"),
    db: AsyncSession = Depends(get_db),
):
    return await get_legacy_state(db, department=department)


@router.get("/historico")
async def legacy_historico(
    days: int = Query(30, ge=1, le=90),
    department: str = Query("Rivera"),
    db: AsyncSession = Depends(get_db),
):
    return await get_legacy_history(db, department=department, days=days)


@router.post("/stats/custom")
async def legacy_stats_custom(geojson: dict, db: AsyncSession = Depends(get_db)):
    return await analyze_custom_geojson(db, geojson)
