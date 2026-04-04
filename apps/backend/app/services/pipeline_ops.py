from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.humedad import AOIUnit
from app.models.materialized import HistoricalStateCache, SatelliteLayerSnapshot
from app.models.pipeline import PipelineRun
from app.services.analysis import (
    _current_analysis_status,
    backfill_department_spatial_cache,
    ensure_latest_daily_analysis,
    recompute_calibrations,
    run_daily_pipeline,
)
from app.services.hexagons import materialize_h3_cache
from app.services.public_api import prewarm_coneat_tiles
from app.services.productive_units import materialize_productive_unit_cache
from app.services.sections import materialize_police_section_cache
from app.services.warehouse import historical_state_cache_key
from app.services.public_api import TEMPORAL_LAYER_CONFIGS


logger = logging.getLogger(__name__)
PIPELINE_RUNTIME_LOCK = asyncio.Lock()

RECALIBRATION_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _scheduler_timezone() -> ZoneInfo:
    return ZoneInfo(settings.default_timezone)


def _serialize_run(row: PipelineRun) -> dict[str, Any]:
    return {
        "id": row.id,
        "job_key": row.job_key,
        "job_type": row.job_type,
        "trigger_source": row.trigger_source,
        "scope": row.scope,
        "department": row.department,
        "target_date": row.target_date.isoformat() if row.target_date else None,
        "scheduled_for": row.scheduled_for.isoformat() if row.scheduled_for else None,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_seconds": round(row.duration_seconds, 1) if row.duration_seconds is not None else None,
        "error_message": row.error_message,
        "details": row.details or {},
    }


def _job_key(
    *,
    job_type: str,
    target_date: date,
    scope: str = "nacional",
    department: str | None = None,
    qualifier: str | None = None,
    force_token: str | None = None,
) -> str:
    base = f"{job_type}::{scope}::{target_date.isoformat()}"
    if department:
        base = f"{base}::{department.lower().replace(' ', '-')}"
    if qualifier:
        base = f"{base}::{qualifier}"
    if force_token:
        base = f"{base}::{force_token}"
    return base


def _scheduled_local(target_date: date, *, weekday: int | None = None) -> datetime:
    tz = _scheduler_timezone()
    local_dt = datetime.combine(
        target_date,
        time(hour=settings.pipeline_cron_hour, minute=settings.pipeline_cron_minute),
        tzinfo=tz,
    )
    if weekday is None:
        return local_dt
    delta = weekday - local_dt.weekday()
    return local_dt + timedelta(days=delta)


def _scheduled_daily_utc(target_date: date) -> datetime:
    return _scheduled_local(target_date).astimezone(timezone.utc)


def _scheduled_weekly_utc(target_date: date) -> datetime:
    weekday = RECALIBRATION_WEEKDAYS.get(settings.recalibration_weekday.lower(), 0)
    return _scheduled_local(target_date, weekday=weekday).astimezone(timezone.utc)


def _next_daily_run_utc(reference: datetime | None = None) -> datetime:
    reference = reference or _now_utc()
    local_now = reference.astimezone(_scheduler_timezone())
    candidate = _scheduled_local(local_now.date())
    if local_now >= candidate:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def _next_recalibration_run_utc(reference: datetime | None = None) -> datetime:
    reference = reference or _now_utc()
    local_now = reference.astimezone(_scheduler_timezone())
    weekday = RECALIBRATION_WEEKDAYS.get(settings.recalibration_weekday.lower(), 0)
    candidate = _scheduled_local(local_now.date(), weekday=weekday)
    while candidate.weekday() != weekday:
        candidate += timedelta(days=1)
    if local_now >= candidate:
        candidate += timedelta(days=7)
    return candidate.astimezone(timezone.utc)


def _due_daily_dates(reference: datetime | None = None) -> list[date]:
    reference = reference or _now_utc()
    local_now = reference.astimezone(_scheduler_timezone())
    due_dates: list[date] = []
    for offset in range(settings.pipeline_bootstrap_backfill_days, -1, -1):
        target = local_now.date() - timedelta(days=offset)
        if _scheduled_local(target) <= local_now:
            due_dates.append(target)
    return due_dates


def _due_recalibration_dates(reference: datetime | None = None) -> list[date]:
    reference = reference or _now_utc()
    local_now = reference.astimezone(_scheduler_timezone())
    weekday = RECALIBRATION_WEEKDAYS.get(settings.recalibration_weekday.lower(), 0)
    due_dates: list[date] = []
    for offset in range(settings.pipeline_bootstrap_backfill_days, -1, -1):
        target = local_now.date() - timedelta(days=offset)
        if target.weekday() != weekday:
            continue
        if _scheduled_local(target) <= local_now:
            due_dates.append(target)
    return due_dates


def _is_stale(row: PipelineRun) -> bool:
    if row.status != "running" or row.started_at is None:
        return False
    started_at = row.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    else:
        started_at = started_at.astimezone(timezone.utc)
    return (_now_utc() - started_at) >= timedelta(hours=settings.pipeline_stale_after_hours)


def _force_token(prefix: str) -> str:
    return f"{prefix}-{_now_utc().strftime('%Y%m%d%H%M%S')}"


def _compact_daily_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_date": result.get("target_date"),
        "processed": result.get("processed"),
        "live_count": result.get("live_count"),
        "carry_forward_count": result.get("carry_forward_count"),
        "simulated_count": result.get("simulated_count"),
        "section_cache_count": result.get("section_cache_count"),
        "hex_cache_count": result.get("hex_cache_count"),
        "productive_cache_count": result.get("productive_cache_count"),
    }


def _compact_recalibration_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_date": result.get("target_date"),
        "calibration_count": len(result.get("calibrations", [])),
    }


def _compact_materialization_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_date": result.get("target_date"),
        "department_cache_count": result.get("department_cache_count"),
        "section_cache_count": result.get("section_cache_count"),
        "hex_cache_count": result.get("hex_cache_count"),
        "productive_cache_count": result.get("productive_cache_count"),
        "department_filter": result.get("department_filter"),
        "coneat_prewarm": result.get("coneat_prewarm"),
    }


def _compact_timeline_backfill_result(result: dict[str, Any], warehouse_status: dict[str, Any]) -> dict[str, Any]:
    return {
        "start_date": result.get("start_date"),
        "end_date": result.get("end_date"),
        "window_days": result.get("window_days"),
        "processed_days": result.get("processed_days"),
        "include_recalibration": result.get("include_recalibration"),
        "warehouse_ready": bool((warehouse_status or {}).get("ready")),
        "warehouse_coverage_pct": (warehouse_status or {}).get("overall_coverage_pct"),
    }


def _coverage_pct(available: int, expected: int) -> float:
    if expected <= 0:
        return 0.0
    return round((available / expected) * 100.0, 1)


def _coverage_status(available: int, expected: int) -> str:
    if expected <= 0 or available <= 0:
        return "missing"
    if available >= expected:
        return "complete"
    return "partial"


def _sample_missing_dates(
    observed_dates: set[date],
    *,
    start_date: date,
    end_date: date,
    limit: int = 5,
) -> list[str]:
    missing: list[str] = []
    current = start_date
    while current <= end_date and len(missing) < limit:
        if current not in observed_dates:
            missing.append(current.isoformat())
        current += timedelta(days=1)
    return missing


async def _build_historical_warehouse_status(
    session: AsyncSession,
    *,
    end_date: date | None = None,
    window_days: int | None = None,
) -> dict[str, Any]:
    end_date = end_date or date.today()
    window_days = max(int(window_days or settings.timeline_historical_window_days), 1)
    start_date = end_date - timedelta(days=window_days - 1)
    window_start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    window_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)

    department_units_result = await session.execute(
        select(AOIUnit.id, AOIUnit.department, AOIUnit.scope)
        .where(
            AOIUnit.active.is_(True),
            AOIUnit.unit_type == "department",
        )
        .order_by(AOIUnit.department)
    )
    department_units = [
        {
            "unit_id": unit_id,
            "department": department,
            "scope": scope or "departamento",
        }
        for unit_id, department, scope in department_units_result.all()
    ]

    national_key = historical_state_cache_key("nacional", department="Uruguay")
    national_rows_result = await session.execute(
        select(HistoricalStateCache.observed_at)
        .where(
            HistoricalStateCache.cache_key == national_key,
            HistoricalStateCache.observed_at >= window_start,
            HistoricalStateCache.observed_at < window_end,
        )
        .order_by(HistoricalStateCache.observed_at)
    )
    national_dates = {
        observed_at.date()
        for (observed_at,) in national_rows_result.all()
        if observed_at is not None
    }

    department_keys = {
        historical_state_cache_key(item["scope"], unit_id=item["unit_id"], department=item["department"]): item
        for item in department_units
    }
    department_dates_by_key: dict[str, set[date]] = defaultdict(set)
    if department_keys:
        department_rows_result = await session.execute(
            select(HistoricalStateCache.cache_key, HistoricalStateCache.observed_at)
            .where(
                HistoricalStateCache.cache_key.in_(list(department_keys.keys())),
                HistoricalStateCache.observed_at >= window_start,
                HistoricalStateCache.observed_at < window_end,
            )
            .order_by(HistoricalStateCache.cache_key, HistoricalStateCache.observed_at)
        )
        for cache_key, observed_at in department_rows_result.all():
            if observed_at is not None:
                department_dates_by_key[cache_key].add(observed_at.date())

    department_items: list[dict[str, Any]] = []
    department_slot_total = window_days * len(department_units)
    department_slot_available = 0
    for cache_key, item in department_keys.items():
        observed_dates = department_dates_by_key.get(cache_key, set())
        available_days = len(observed_dates)
        department_slot_available += available_days
        latest_date = max(observed_dates).isoformat() if observed_dates else None
        department_items.append(
            {
                "department": item["department"],
                "unit_id": item["unit_id"],
                "available_days": available_days,
                "expected_days": window_days,
                "coverage_pct": _coverage_pct(available_days, window_days),
                "status": _coverage_status(available_days, window_days),
                "latest_observed_date": latest_date,
                "missing_sample": _sample_missing_dates(
                    observed_dates,
                    start_date=start_date,
                    end_date=end_date,
                ),
            }
        )

    department_unit_ids = [str(item["unit_id"]) for item in department_units]
    layer_dates_by_key: dict[str, set[date]] = defaultdict(set)
    if department_unit_ids:
        layer_rows_result = await session.execute(
            select(SatelliteLayerSnapshot.layer_key, SatelliteLayerSnapshot.observed_at)
            .where(
                SatelliteLayerSnapshot.unit_id.in_(department_unit_ids),
                SatelliteLayerSnapshot.layer_key.in_(list(TEMPORAL_LAYER_CONFIGS.keys())),
                SatelliteLayerSnapshot.observed_at >= window_start,
                SatelliteLayerSnapshot.observed_at < window_end,
            )
            .distinct()
        )
        for layer_key, observed_at in layer_rows_result.all():
            if observed_at is not None:
                layer_dates_by_key[str(layer_key)].add(observed_at.date())

    layer_items: list[dict[str, Any]] = []
    for layer_key, config in TEMPORAL_LAYER_CONFIGS.items():
        observed_dates = layer_dates_by_key.get(layer_key, set())
        available_days = len(observed_dates)
        latest_date = max(observed_dates).isoformat() if observed_dates else None
        layer_items.append(
            {
                "layer_id": str(config.get("public_id") or layer_key),
                "layer_key": layer_key,
                "label": str(config.get("label") or layer_key),
                "available_days": available_days,
                "expected_days": window_days,
                "coverage_pct": _coverage_pct(available_days, window_days),
                "status": _coverage_status(available_days, window_days),
                "latest_observed_date": latest_date,
                "missing_sample": _sample_missing_dates(
                    observed_dates,
                    start_date=start_date,
                    end_date=end_date,
                ),
            }
        )

    national_available_days = len(national_dates)
    national_status = _coverage_status(national_available_days, window_days)
    national_summary = {
        "available_days": national_available_days,
        "expected_days": window_days,
        "coverage_pct": _coverage_pct(national_available_days, window_days),
        "status": national_status,
        "latest_observed_date": max(national_dates).isoformat() if national_dates else None,
        "missing_sample": _sample_missing_dates(
            national_dates,
            start_date=start_date,
            end_date=end_date,
        ),
    }

    complete_departments = sum(1 for item in department_items if item["status"] == "complete")
    complete_layers = sum(1 for item in layer_items if item["status"] == "complete")
    expected_total_slots = window_days * (1 + len(department_items) + len(layer_items))
    available_total_slots = national_available_days + department_slot_available + sum(item["available_days"] for item in layer_items)
    overall_coverage_pct = _coverage_pct(available_total_slots, expected_total_slots)

    return {
        "window_days": window_days,
        "date_from": start_date.isoformat(),
        "date_to": end_date.isoformat(),
        "ready": bool(
            national_status == "complete"
            and complete_departments == len(department_items)
            and complete_layers == len(layer_items)
        ),
        "overall_coverage_pct": overall_coverage_pct,
        "national": national_summary,
        "departments": {
            "expected_departments": len(department_items),
            "fully_covered_departments": complete_departments,
            "available_day_slots": department_slot_available,
            "expected_day_slots": department_slot_total,
            "coverage_pct": _coverage_pct(department_slot_available, department_slot_total),
            "items": department_items,
        },
        "temporal_layers": {
            "expected_layers": len(layer_items),
            "fully_covered_layers": complete_layers,
            "items": layer_items,
        },
    }


async def _get_run_by_key(session: AsyncSession, job_key: str) -> PipelineRun | None:
    result = await session.execute(select(PipelineRun).where(PipelineRun.job_key == job_key).limit(1))
    return result.scalar_one_or_none()


async def _claim_run(
    session: AsyncSession,
    *,
    job_type: str,
    target_date: date,
    trigger_source: str,
    scope: str = "nacional",
    department: str | None = None,
    scheduled_for: datetime | None = None,
    qualifier: str | None = None,
    force: bool = False,
) -> tuple[PipelineRun, bool, str]:
    job_key = _job_key(
        job_type=job_type,
        target_date=target_date,
        scope=scope,
        department=department,
        qualifier=qualifier,
        force_token=_force_token(trigger_source) if force else None,
    )
    row = await _get_run_by_key(session, job_key)
    if row is None and not force:
        row = await _get_run_by_key(
            session,
            _job_key(
                job_type=job_type,
                target_date=target_date,
                scope=scope,
                department=department,
                qualifier=qualifier,
            ),
        )
        job_key = row.job_key if row else job_key

    if row is not None:
        if row.status == "success" and not force:
            return row, False, "already_success"
        if row.status == "running" and not _is_stale(row):
            return row, False, "already_running"
        if row.status == "running" and _is_stale(row):
            row.status = "failed"
            row.finished_at = _now_utc()
            row.duration_seconds = (row.finished_at - row.started_at).total_seconds() if row.started_at else None
            row.error_message = "Marked stale before retry"

    if row is None:
        row = PipelineRun(
            job_key=job_key,
            job_type=job_type,
            trigger_source=trigger_source,
            scope=scope,
            department=department,
            target_date=target_date,
            scheduled_for=scheduled_for,
        )
        session.add(row)

    row.job_type = job_type
    row.trigger_source = trigger_source
    row.scope = scope
    row.department = department
    row.target_date = target_date
    row.scheduled_for = scheduled_for
    row.status = "running"
    row.started_at = _now_utc()
    row.finished_at = None
    row.duration_seconds = None
    row.error_message = None
    row.details = {"scope": scope, "department": department}
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await _get_run_by_key(session, job_key)
        if existing is not None:
            if existing.status == "running":
                return existing, False, "already_running"
            if existing.status == "success" and not force:
                return existing, False, "already_success"
        raise
    return row, True, "claimed"


async def _finalize_run(
    session: AsyncSession,
    *,
    run_id: str,
    status: str,
    details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> PipelineRun:
    row = await session.get(PipelineRun, run_id)
    if row is None:
        raise ValueError(f"Pipeline run no encontrado: {run_id}")
    row.status = status
    row.finished_at = _now_utc()
    if row.started_at:
        started_at = row.started_at if row.started_at.tzinfo else row.started_at.replace(tzinfo=timezone.utc)
        row.duration_seconds = (row.finished_at - started_at.astimezone(timezone.utc)).total_seconds()
    row.details = details or row.details or {}
    row.error_message = error_message
    await session.commit()
    await session.refresh(row)
    return row


async def list_pipeline_runs(
    session: AsyncSession,
    *,
    limit: int = 20,
    job_type: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    query = select(PipelineRun).order_by(desc(PipelineRun.started_at), desc(PipelineRun.created_at)).limit(limit)
    if job_type:
        query = query.where(PipelineRun.job_type == job_type)
    if status:
        query = query.where(PipelineRun.status == status)
    result = await session.execute(query)
    return [_serialize_run(row) for row in result.scalars().all()]


async def get_pipeline_status(session: AsyncSession) -> dict[str, Any]:
    today = date.today()
    analysis_status = await _current_analysis_status(session, today)
    recent_runs = await list_pipeline_runs(session, limit=12)
    historical_warehouse = await _build_historical_warehouse_status(
        session,
        end_date=today,
        window_days=settings.timeline_historical_window_days,
    )
    result = await session.execute(
        select(PipelineRun)
        .where(PipelineRun.job_type.in_(["daily_pipeline", "weekly_recalibration"]))
        .order_by(desc(PipelineRun.started_at), desc(PipelineRun.created_at))
    )
    rows = list(result.scalars().all())
    last_daily_success = next((row for row in rows if row.job_type == "daily_pipeline" and row.status == "success"), None)
    last_recal_success = next((row for row in rows if row.job_type == "weekly_recalibration" and row.status == "success"), None)

    start_window = today - timedelta(days=settings.pipeline_bootstrap_backfill_days)
    window_rows_result = await session.execute(
        select(PipelineRun).where(PipelineRun.target_date >= start_window, PipelineRun.target_date <= today)
    )
    window_rows = list(window_rows_result.scalars().all())
    successful_daily_dates = {
        row.target_date
        for row in window_rows
        if row.job_type == "daily_pipeline" and row.status == "success"
    }
    pending_backfill_dates = [item.isoformat() for item in _due_daily_dates() if item not in successful_daily_dates]

    return {
        **analysis_status,
        "scheduler": {
            "enabled": settings.pipeline_scheduler_enabled,
            "timezone": settings.default_timezone,
            "poll_seconds": settings.pipeline_scheduler_poll_seconds,
            "bootstrap_backfill_days": settings.pipeline_bootstrap_backfill_days,
            "timeline_historical_window_days": settings.timeline_historical_window_days,
            "next_daily_run": _next_daily_run_utc().isoformat(),
            "next_recalibration_run": _next_recalibration_run_utc().isoformat(),
        },
        "runs": {
            "last_daily_success": _serialize_run(last_daily_success) if last_daily_success else None,
            "last_recalibration_success": _serialize_run(last_recal_success) if last_recal_success else None,
            "recent": recent_runs,
        },
        "pending_backfill_dates": pending_backfill_dates,
        "historical_warehouse": historical_warehouse,
    }


async def execute_daily_pipeline_job(
    session: AsyncSession,
    *,
    target_date: date | None = None,
    trigger_source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    run_row, claimed, status = await _claim_run(
        session,
        job_type="daily_pipeline",
        target_date=target_date,
        trigger_source=trigger_source,
        scheduled_for=_scheduled_daily_utc(target_date),
        force=force,
    )
    if not claimed:
        return {"status": status, "job": _serialize_run(run_row)}

    async with PIPELINE_RUNTIME_LOCK:
        try:
            is_current_target = target_date >= date.today()
            result = await run_daily_pipeline(
                session,
                target_date=target_date,
                update_current_state=is_current_target,
                materialize_latest=is_current_target,
                refresh_catalog_geometries=is_current_target,
            )
            if is_current_target and settings.coneat_prewarm_enabled:
                result["coneat_prewarm"] = await prewarm_coneat_tiles()
        except Exception as exc:
            await session.rollback()
            finalized = await _finalize_run(
                session,
                run_id=run_row.id,
                status="failed",
                details={"target_date": target_date.isoformat()},
                error_message=str(exc),
            )
            raise RuntimeError(f"Fallo daily pipeline para {target_date.isoformat()}") from exc

    finalized = await _finalize_run(
        session,
        run_id=run_row.id,
        status="success",
        details=_compact_daily_result(result),
    )
    return {"status": "success", "job": _serialize_run(finalized), "result": result}


async def execute_recalibration_job(
    session: AsyncSession,
    *,
    target_date: date | None = None,
    trigger_source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    run_row, claimed, status = await _claim_run(
        session,
        job_type="weekly_recalibration",
        target_date=target_date,
        trigger_source=trigger_source,
        scheduled_for=_scheduled_weekly_utc(target_date),
        force=force,
    )
    if not claimed:
        return {"status": status, "job": _serialize_run(run_row)}

    async with PIPELINE_RUNTIME_LOCK:
        try:
            result = await recompute_calibrations(session, as_of=target_date)
        except Exception as exc:
            await session.rollback()
            await _finalize_run(
                session,
                run_id=run_row.id,
                status="failed",
                details={"target_date": target_date.isoformat()},
                error_message=str(exc),
            )
            raise RuntimeError(f"Fallo recalibracion para {target_date.isoformat()}") from exc

    finalized = await _finalize_run(
        session,
        run_id=run_row.id,
        status="success",
        details=_compact_recalibration_result(result),
    )
    return {"status": "success", "job": _serialize_run(finalized), "result": result}


async def refresh_materialized_layers(
    session: AsyncSession,
    *,
    department: str | None = None,
    target_date: date | None = None,
    trigger_source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    scope = department or "nacional"
    run_row, claimed, status = await _claim_run(
        session,
        job_type="materialization_refresh",
        target_date=target_date,
        trigger_source=trigger_source,
        scope=scope,
        department=department,
        scheduled_for=_now_utc(),
        force=force,
    )
    if not claimed:
        return {"status": status, "job": _serialize_run(run_row)}

    async with PIPELINE_RUNTIME_LOCK:
        try:
            if target_date >= date.today():
                await ensure_latest_daily_analysis(session, target_date=target_date)
            department_result = await backfill_department_spatial_cache(session, department=department)
            section_result = await materialize_police_section_cache(
                session,
                target_date=target_date,
                department=department,
                ensure_base_analysis=False,
                persist_latest=True,
            )
            hex_result = await materialize_h3_cache(
                session,
                target_date=target_date,
                department=department,
                ensure_base_analysis=False,
                persist_latest=True,
            )
            productive_result = await materialize_productive_unit_cache(
                session,
                target_date=target_date,
                department=department,
                ensure_base_analysis=False,
                persist_latest=True,
            )
            await session.commit()
            result = {
                "target_date": target_date.isoformat(),
                "department_filter": department,
                "department_cache_count": department_result.get("count", 0),
                "section_cache_count": section_result.get("count", 0),
                "hex_cache_count": hex_result.get("count", 0),
                "productive_cache_count": productive_result.get("count", 0),
            }
            if target_date >= date.today() and settings.coneat_prewarm_enabled:
                result["coneat_prewarm"] = await prewarm_coneat_tiles(department=department)
        except Exception as exc:
            await session.rollback()
            await _finalize_run(
                session,
                run_id=run_row.id,
                status="failed",
                details={"target_date": target_date.isoformat(), "department_filter": department},
                error_message=str(exc),
            )
            raise RuntimeError("Fallo la materializacion de capas") from exc

    finalized = await _finalize_run(
        session,
        run_id=run_row.id,
        status="success",
        details=_compact_materialization_result(result),
    )
    return {"status": "success", "job": _serialize_run(finalized), "result": result}


async def run_historical_backfill(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    include_recalibration: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    if end_date < start_date:
        raise ValueError("La fecha final no puede ser anterior a la inicial")
    processed_days = 0
    executed: list[dict[str, Any]] = []
    current_date = start_date
    while current_date <= end_date:
        daily_result = await execute_daily_pipeline_job(
            session,
            target_date=current_date,
            trigger_source="backfill",
            force=force,
        )
        executed.append({"date": current_date.isoformat(), "daily": daily_result["status"]})
        if include_recalibration and current_date.weekday() == RECALIBRATION_WEEKDAYS.get(settings.recalibration_weekday.lower(), 0):
            recal_result = await execute_recalibration_job(
                session,
                target_date=current_date,
                trigger_source="backfill",
                force=force,
            )
            executed[-1]["recalibration"] = recal_result["status"]
        processed_days += 1
        current_date += timedelta(days=1)
    return {
        "status": "success",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "window_days": (end_date - start_date).days + 1,
        "processed_days": processed_days,
        "include_recalibration": include_recalibration,
        "runs": executed,
    }


async def execute_timeline_backfill_job(
    session: AsyncSession,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    window_days: int | None = None,
    include_recalibration: bool = True,
    trigger_source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    end_date = end_date or date.today()
    resolved_window_days = max(int(window_days or settings.timeline_historical_window_days), 1)
    start_date = start_date or (end_date - timedelta(days=resolved_window_days - 1))
    if end_date < start_date:
        raise ValueError("La fecha final no puede ser anterior a la inicial")

    qualifier = f"{start_date.isoformat()}::{end_date.isoformat()}::{resolved_window_days}"
    run_row, claimed, status = await _claim_run(
        session,
        job_type="timeline_backfill",
        target_date=end_date,
        trigger_source=trigger_source,
        scheduled_for=_now_utc(),
        force=force,
        qualifier=qualifier,
    )
    if not claimed:
        warehouse_status = await _build_historical_warehouse_status(
            session,
            end_date=end_date,
            window_days=resolved_window_days,
        )
        return {
            "status": status,
            "job": _serialize_run(run_row),
            "warehouse": warehouse_status,
        }

    try:
        result = await run_historical_backfill(
            session,
            start_date=start_date,
            end_date=end_date,
            include_recalibration=include_recalibration,
            force=force,
        )
        warehouse_status = await _build_historical_warehouse_status(
            session,
            end_date=end_date,
            window_days=resolved_window_days,
        )
    except Exception as exc:
        await session.rollback()
        await _finalize_run(
            session,
            run_id=run_row.id,
            status="failed",
            details={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "window_days": resolved_window_days,
                "include_recalibration": include_recalibration,
            },
            error_message=str(exc),
        )
        raise RuntimeError("Fallo el backfill historico de timeline") from exc

    finalized = await _finalize_run(
        session,
        run_id=run_row.id,
        status="success",
        details=_compact_timeline_backfill_result(result, warehouse_status),
    )
    return {
        "status": "success",
        "job": _serialize_run(finalized),
        "result": result,
        "warehouse": warehouse_status,
    }


async def run_due_scheduled_jobs(session: AsyncSession, *, reference: datetime | None = None) -> dict[str, Any]:
    reference = reference or _now_utc()
    executed: list[dict[str, Any]] = []
    for target_date in _due_daily_dates(reference):
        result = await execute_daily_pipeline_job(
            session,
            target_date=target_date,
            trigger_source="scheduler",
            force=False,
        )
        executed.append({"job_type": "daily_pipeline", "target_date": target_date.isoformat(), "status": result["status"]})

    for target_date in _due_recalibration_dates(reference):
        result = await execute_recalibration_job(
            session,
            target_date=target_date,
            trigger_source="scheduler",
            force=False,
        )
        executed.append({"job_type": "weekly_recalibration", "target_date": target_date.isoformat(), "status": result["status"]})

    return {"status": "ok", "executed": executed, "reference": reference.isoformat()}


async def execute_coneat_prewarm_job(
    session: AsyncSession,
    *,
    department: str | None = None,
    trigger_source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    target_date = date.today()
    scope = department or "nacional"
    run_row, claimed, status = await _claim_run(
        session,
        job_type="coneat_prewarm",
        target_date=target_date,
        trigger_source=trigger_source,
        scope=scope,
        department=department,
        scheduled_for=_now_utc(),
        force=force,
    )
    if not claimed:
        return {"status": status, "job": _serialize_run(run_row)}

    async with PIPELINE_RUNTIME_LOCK:
        try:
            result = await prewarm_coneat_tiles(department=department)
        except Exception as exc:
            await session.rollback()
            await _finalize_run(
                session,
                run_id=run_row.id,
                status="failed",
                details={"department_filter": department},
                error_message=str(exc),
            )
            raise RuntimeError("Fallo el precache de CONEAT") from exc

    finalized = await _finalize_run(
        session,
        run_id=run_row.id,
        status="success",
        details=result,
    )
    return {"status": "success", "job": _serialize_run(finalized), "result": result}


async def scheduler_loop() -> None:
    while True:
        try:
            async with AsyncSessionLocal() as session:
                result = await run_due_scheduled_jobs(session)
                if result["executed"]:
                    logger.info("Scheduler ejecuto %s corridas", len(result["executed"]))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Fallo el scheduler interno del pipeline")
        await asyncio.sleep(settings.pipeline_scheduler_poll_seconds)


async def stop_scheduler(task: asyncio.Task[Any] | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
