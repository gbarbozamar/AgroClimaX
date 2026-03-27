from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.pipeline_ops import (
    execute_coneat_prewarm_job,
    execute_daily_pipeline_job,
    execute_recalibration_job,
    get_pipeline_status,
    list_pipeline_runs,
    refresh_materialized_layers,
    run_due_scheduled_jobs,
    run_historical_backfill,
)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.post("/ejecutar")
async def ejecutar_pipeline(
    fecha: date | None = Query(None),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await execute_daily_pipeline_job(db, target_date=fecha, trigger_source="manual", force=force)


@router.post("/recalibrar")
async def recalibrar(
    fecha: date | None = Query(None),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await execute_recalibration_job(db, target_date=fecha, trigger_source="manual", force=force)


@router.post("/materializar")
async def materializar_capas(
    fecha: date | None = Query(None),
    department: str | None = Query(None),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await refresh_materialized_layers(
        db,
        target_date=fecha,
        department=department,
        trigger_source="manual",
        force=force,
    )


@router.post("/prewarm-coneat")
async def prewarm_coneat(
    department: str | None = Query(None),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await execute_coneat_prewarm_job(
        db,
        department=department,
        trigger_source="manual",
        force=force,
    )


@router.post("/backfill")
async def backfill_historico(
    fecha_desde: date = Query(...),
    fecha_hasta: date = Query(...),
    incluir_recalibracion: bool = Query(True),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await run_historical_backfill(
        db,
        start_date=fecha_desde,
        end_date=fecha_hasta,
        include_recalibration=incluir_recalibracion,
        force=force,
    )


@router.post("/scheduler/tick")
async def scheduler_tick(db: AsyncSession = Depends(get_db)):
    return await run_due_scheduled_jobs(db)


@router.get("/runs")
async def pipeline_runs(
    limit: int = Query(20, ge=1, le=100),
    job_type: str | None = Query(None),
    status: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_pipeline_runs(db, limit=limit, job_type=job_type, status=status)
    return {"total": len(rows), "datos": rows}


@router.get("/estado")
async def estado_pipeline(db: AsyncSession = Depends(get_db)):
    return await get_pipeline_status(db)
