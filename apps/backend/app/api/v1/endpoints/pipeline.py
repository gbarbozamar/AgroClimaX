from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.analysis import ensure_latest_daily_analysis, recompute_calibrations, run_daily_pipeline

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.post("/ejecutar")
async def ejecutar_pipeline(
    fecha: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await run_daily_pipeline(db, target_date=fecha)


@router.post("/recalibrar")
async def recalibrar(
    fecha: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await recompute_calibrations(db, as_of=fecha)


@router.get("/estado")
async def estado_pipeline(db: AsyncSession = Depends(get_db)):
    return await ensure_latest_daily_analysis(db)
