from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.pipeline_ops import (
    execute_coneat_prewarm_job,
    execute_aoi_stats_backfill_job,
    execute_daily_pipeline_job,
    execute_raster_daily_refresh_job,
    execute_raster_backfill_job,
    execute_recalibration_job,
    execute_scene_catalog_sync_job,
    execute_stage2_backfill_job,
    execute_timeline_backfill_job,
    get_raster_status,
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


@router.post("/backfill-timeline")
async def backfill_timeline_historico(
    fecha_hasta: date | None = Query(None),
    fecha_desde: date | None = Query(None),
    window_days: int | None = Query(None, ge=1, le=365),
    incluir_recalibracion: bool = Query(True),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await execute_timeline_backfill_job(
        db,
        start_date=fecha_desde,
        end_date=fecha_hasta,
        window_days=window_days,
        include_recalibration=incluir_recalibracion,
        trigger_source="manual",
        force=force,
    )


@router.post("/backfill-scenes")
async def backfill_scenes(
    fecha_desde: date | None = Query(None),
    fecha_hasta: date | None = Query(None),
    departments: list[str] = Query(default_factory=list),
    collections: list[str] = Query(default_factory=list),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    end_date = fecha_hasta or date.today()
    start_date = fecha_desde or (end_date - timedelta(days=29))
    return await execute_scene_catalog_sync_job(
        db,
        start_date=start_date,
        end_date=end_date,
        departments=departments or None,
        collections=collections or None,
        trigger_source="manual",
        force=force,
    )


@router.post("/backfill-raster-products")
async def backfill_raster_products_endpoint(
    fecha_desde: date | None = Query(None),
    fecha_hasta: date | None = Query(None),
    departments: list[str] = Query(default_factory=list),
    layers: list[str] = Query(default_factory=list),
    include_national: bool = Query(True),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    end_date = fecha_hasta or date.today()
    start_date = fecha_desde or (end_date - timedelta(days=29))
    return await execute_raster_backfill_job(
        db,
        start_date=start_date,
        end_date=end_date,
        departments=departments or None,
        layers=layers or None,
        include_national=include_national,
        trigger_source="manual",
        force=force,
    )


@router.post("/backfill-aoi-stats")
async def backfill_aoi_stats_endpoint(
    fecha_desde: date | None = Query(None),
    fecha_hasta: date | None = Query(None),
    layers: list[str] = Query(default_factory=list),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    end_date = fecha_hasta or date.today()
    start_date = fecha_desde or (end_date - timedelta(days=29))
    return await execute_aoi_stats_backfill_job(
        db,
        start_date=start_date,
        end_date=end_date,
        layers=layers or None,
        trigger_source="manual",
        force=force,
    )


@router.post("/refresh-raster")
async def refresh_raster_endpoint(
    fecha: date | None = Query(None),
    departments: list[str] = Query(default_factory=list),
    layers: list[str] = Query(default_factory=list),
    include_national: bool = Query(True),
    warm: bool = Query(True),
    warm_days: int | None = Query(None, ge=0, le=365),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await execute_raster_daily_refresh_job(
        db,
        target_date=fecha,
        departments=departments or None,
        layers=layers or None,
        include_national=include_national,
        warm=warm,
        warm_days=warm_days,
        trigger_source="manual",
        force=force,
    )


@router.post("/backfill-stage2")
async def backfill_stage2_endpoint(
    fecha_hasta: date | None = Query(None),
    window_days: int | None = Query(365, ge=1, le=365),
    departments: list[str] = Query(default_factory=list),
    layers: list[str] = Query(default_factory=list),
    include_national: bool = Query(True),
    warm_days: int | None = Query(0, ge=0, le=365),
    force: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    return await execute_stage2_backfill_job(
        db,
        end_date=fecha_hasta,
        window_days=window_days,
        departments=departments or None,
        layers=layers or None,
        include_national=include_national,
        warm_days=warm_days,
        trigger_source="manual",
        force=force,
    )


@router.get("/raster-status")
async def raster_status(
    window_days: int = Query(365, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
):
    return await get_raster_status(db, window_days=window_days)


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
