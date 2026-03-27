from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.analysis import analyze_custom_geojson, get_alert_history, get_scope_snapshot, get_scope_weather_forecast

router = APIRouter(prefix="/alertas", tags=["alertas"])


@router.get("/estado-actual")
async def estado_actual(
    scope: str = Query("departamento", pattern="^(nacional|departamento|unidad)$"),
    unit_id: str | None = Query(None),
    department: str | None = Query("Rivera"),
    db: AsyncSession = Depends(get_db),
):
    return await get_scope_snapshot(db, scope=scope, unit_id=unit_id, department=department)


@router.get("/historico")
async def historico(
    scope: str = Query("departamento", pattern="^(nacional|departamento|unidad)$"),
    unit_id: str | None = Query(None),
    department: str | None = Query("Rivera"),
    limit: int = Query(30, ge=1, le=180),
    db: AsyncSession = Depends(get_db),
):
    return await get_alert_history(db, scope=scope, unit_id=unit_id, department=department, limit=limit)


@router.get("/pronostico")
async def pronostico(
    scope: str = Query("departamento", pattern="^(nacional|departamento|unidad)$"),
    unit_id: str | None = Query(None),
    department: str | None = Query("Rivera"),
    db: AsyncSession = Depends(get_db),
):
    return await get_scope_weather_forecast(db, scope=scope, unit_id=unit_id, department=department)


@router.post("/unidad/custom")
async def unidad_custom(geojson: dict, db: AsyncSession = Depends(get_db)):
    return await analyze_custom_geojson(db, geojson)
