from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.auth import require_integration_service_request
from app.services.mcp_integration import (
    get_active_weather_alerts_for_mcp,
    get_field_alert_history_for_mcp,
    get_field_current_status_for_mcp,
    get_field_historical_trend_for_mcp,
    get_field_metadata_for_mcp,
    get_field_timeline_context_for_mcp,
    get_latest_satellite_coverage_for_mcp,
    get_paddock_alert_history_for_mcp,
    get_paddock_current_status_for_mcp,
    get_paddock_historical_trend_for_mcp,
    get_paddock_metadata_for_mcp,
    get_paddock_timeline_context_for_mcp,
    search_fields_for_mcp,
    search_paddocks_for_mcp,
)


router = APIRouter(
    prefix="/integrations/mcp",
    tags=["integrations-mcp"],
    dependencies=[Depends(require_integration_service_request)],
)


def _translate_service_error(exc: Exception) -> HTTPException:
    detail = str(exc)
    if "aoi_unit_id" in detail.lower():
        return HTTPException(status_code=409, detail=detail)
    return HTTPException(status_code=404, detail=detail)


@router.get("/fields/search")
async def fields_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(5, ge=1, le=25),
    db: AsyncSession = Depends(get_db),
):
    return await search_fields_for_mcp(db, query=q, limit=limit)


@router.get("/fields/{field_id}")
async def field_detail(
    field_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_field_metadata_for_mcp(db, field_id=field_id)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/fields/{field_id}/current-status")
async def field_current_status(
    field_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_field_current_status_for_mcp(db, field_id=field_id)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/fields/{field_id}/timeline-context")
async def field_timeline_context(
    field_id: str,
    target_date: date = Query(...),
    history_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_field_timeline_context_for_mcp(
            db,
            field_id=field_id,
            target_date=target_date,
            history_days=history_days,
        )
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/fields/{field_id}/historical-trend")
async def field_historical_trend(
    field_id: str,
    days: int = Query(30, ge=1, le=180),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_field_historical_trend_for_mcp(db, field_id=field_id, days=days)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/fields/{field_id}/latest-satellite-coverage")
async def field_latest_satellite_coverage(
    field_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_latest_satellite_coverage_for_mcp(db, field_id=field_id)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/fields/{field_id}/active-weather-alerts")
async def field_active_weather_alerts(
    field_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_active_weather_alerts_for_mcp(db, field_id=field_id)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/fields/{field_id}/alert-history")
async def field_alert_history(
    field_id: str,
    days: int = Query(30, ge=1, le=180),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_field_alert_history_for_mcp(db, field_id=field_id, days=days)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/paddocks/search")
async def paddocks_search(
    q: str = Query(..., min_length=1),
    field_id: str | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    return await search_paddocks_for_mcp(db, query=q, field_id=field_id, limit=limit)


@router.get("/paddocks/{paddock_id}")
async def paddock_detail(
    paddock_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_paddock_metadata_for_mcp(db, paddock_id=paddock_id)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/paddocks/{paddock_id}/current-status")
async def paddock_current_status(
    paddock_id: str,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_paddock_current_status_for_mcp(db, paddock_id=paddock_id)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/paddocks/{paddock_id}/timeline-context")
async def paddock_timeline_context(
    paddock_id: str,
    target_date: date = Query(...),
    history_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_paddock_timeline_context_for_mcp(
            db,
            paddock_id=paddock_id,
            target_date=target_date,
            history_days=history_days,
        )
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/paddocks/{paddock_id}/historical-trend")
async def paddock_historical_trend(
    paddock_id: str,
    days: int = Query(30, ge=1, le=180),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_paddock_historical_trend_for_mcp(db, paddock_id=paddock_id, days=days)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc


@router.get("/paddocks/{paddock_id}/alert-history")
async def paddock_alert_history(
    paddock_id: str,
    days: int = Query(30, ge=1, le=180),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_paddock_alert_history_for_mcp(db, paddock_id=paddock_id, days=days)
    except (ValueError, RuntimeError) as exc:
        raise _translate_service_error(exc) from exc
