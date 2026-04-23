from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, desc, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.materialized import PreloadRun, RasterCacheEntry


def _build_insert(session: AsyncSession, model):
    dialect_name = session.bind.dialect.name if session.bind is not None else ""
    if dialect_name == "sqlite":
        return sqlite_insert(model)
    if dialect_name == "postgresql":
        return postgresql_insert(model)
    return None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _date_to_datetime(value: date | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value)
        except Exception:
            return None
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _bbox_step_for_zoom(zoom: int | None) -> float:
    resolved_zoom = int(zoom or 0)
    if resolved_zoom >= 14:
        return 0.005
    if resolved_zoom >= 11:
        return 0.02
    if resolved_zoom >= 8:
        return 0.1
    return 0.25


def _format_bucket_value(value: float, step: float) -> str:
    precision = 3
    if step <= 0.02:
        precision = 4
    if step <= 0.005:
        precision = 5
    return f"{value:.{precision}f}"


def parse_bbox_values(bbox: str | tuple[float, float, float, float] | list[float] | None) -> tuple[float, float, float, float] | None:
    if bbox is None:
        return None
    if isinstance(bbox, str):
        parts = [part.strip() for part in bbox.split(",")[:4]]
        if len(parts) != 4:
            return None
        try:
            west, south, east, north = [float(part) for part in parts]
        except Exception:
            return None
        return west, south, east, north
    if isinstance(bbox, (tuple, list)) and len(bbox) >= 4:
        try:
            west, south, east, north = [float(item) for item in bbox[:4]]
        except Exception:
            return None
        return west, south, east, north
    return None


def viewport_bucket(
    bbox: str | tuple[float, float, float, float] | list[float] | None,
    *,
    zoom: int | None = None,
    width: int | None = None,
    height: int | None = None,
) -> str:
    resolved = parse_bbox_values(bbox)
    if resolved is None:
        return "auto"
    west, south, east, north = resolved
    step = _bbox_step_for_zoom(zoom)
    bucketed = []
    for value in (west, south, east, north):
        snapped = round(value / step) * step
        bucketed.append(_format_bucket_value(snapped, step))
    if width is None and height is None:
        return ",".join(bucketed)
    width_bucket = int(round((width or 256) / 128.0) * 128)
    height_bucket = int(round((height or 256) / 128.0) * 128)
    return f"{','.join(bucketed)}@{width_bucket}x{height_bucket}"


def tile_bbox_bucket(z: int, x: int, y: int) -> str:
    return f"tile:{z}:{x}:{y}"


def raster_cache_key(
    *,
    cache_kind: str,
    layer_id: str,
    display_date: date | str | None = None,
    source_date: date | str | None = None,
    zoom: int | None = None,
    bbox_bucket: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> str:
    parts = [
        cache_kind,
        layer_id,
        str(display_date or "-"),
        str(source_date or "-"),
        str(zoom if zoom is not None else "-"),
        str(bbox_bucket or "-"),
        str(scope_type or "-"),
        str(scope_ref or "-"),
    ]
    return "::".join(parts)


async def upsert_raster_cache_entry(
    session: AsyncSession,
    *,
    cache_key: str,
    layer_id: str,
    cache_kind: str,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    display_date: date | str | None = None,
    source_date: date | str | None = None,
    zoom: int | None = None,
    bbox_bucket: str | None = None,
    storage_backend: str = "filesystem",
    storage_key: str | None = None,
    status: str = "ready",
    bytes_size: int | None = None,
    metadata_extra: dict[str, Any] | None = None,
    last_warmed_at: datetime | None = None,
    last_hit_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> RasterCacheEntry:
    now = _now_utc()
    values = {
        "id": str(uuid4()),
        "cache_key": cache_key,
        "layer_id": layer_id,
        "cache_kind": cache_kind,
        "scope_type": scope_type,
        "scope_ref": scope_ref,
        "display_date": _date_to_datetime(display_date),
        "source_date": _date_to_datetime(source_date),
        "zoom": zoom,
        "bbox_bucket": bbox_bucket,
        "storage_backend": storage_backend,
        "storage_key": storage_key,
        "status": status,
        "bytes_size": bytes_size,
        "metadata_extra": metadata_extra or {},
        "last_warmed_at": last_warmed_at,
        "last_hit_at": last_hit_at,
        "expires_at": expires_at,
        "updated_at": now,
    }
    insert_stmt = _build_insert(session, RasterCacheEntry)
    if insert_stmt is not None:
        await session.execute(
            insert_stmt.values(**values).on_conflict_do_update(
                index_elements=[RasterCacheEntry.cache_key],
                set_={
                    "layer_id": layer_id,
                    "cache_kind": cache_kind,
                    "scope_type": scope_type,
                    "scope_ref": scope_ref,
                    "display_date": values["display_date"],
                    "source_date": values["source_date"],
                    "zoom": zoom,
                    "bbox_bucket": bbox_bucket,
                    "storage_backend": storage_backend,
                    "storage_key": storage_key,
                    "status": status,
                    "bytes_size": bytes_size,
                    "metadata_extra": metadata_extra or {},
                    "last_warmed_at": last_warmed_at,
                    "last_hit_at": last_hit_at,
                    "expires_at": expires_at,
                    "updated_at": now,
                },
            )
        )
        await session.flush()
    else:
        result = await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key).limit(1))
        row = result.scalar_one_or_none()
        if row is None:
            row = RasterCacheEntry(cache_key=cache_key, layer_id=layer_id, cache_kind=cache_kind)
            session.add(row)
        row.scope_type = scope_type
        row.scope_ref = scope_ref
        row.display_date = values["display_date"]
        row.source_date = values["source_date"]
        row.zoom = zoom
        row.bbox_bucket = bbox_bucket
        row.storage_backend = storage_backend
        row.storage_key = storage_key
        row.status = status
        row.bytes_size = bytes_size
        row.metadata_extra = metadata_extra or {}
        row.last_warmed_at = last_warmed_at
        row.last_hit_at = last_hit_at
        row.expires_at = expires_at
        row.updated_at = now
        await session.flush()
    result = await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key).limit(1))
    return result.scalar_one()


async def touch_raster_cache_hit(
    session: AsyncSession,
    *,
    cache_key: str,
    layer_id: str,
    cache_kind: str,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    display_date: date | str | None = None,
    source_date: date | str | None = None,
    zoom: int | None = None,
    bbox_bucket: str | None = None,
    storage_backend: str = "filesystem",
    storage_key: str | None = None,
    metadata_extra: dict[str, Any] | None = None,
) -> RasterCacheEntry:
    return await upsert_raster_cache_entry(
        session,
        cache_key=cache_key,
        layer_id=layer_id,
        cache_kind=cache_kind,
        scope_type=scope_type,
        scope_ref=scope_ref,
        display_date=display_date,
        source_date=source_date,
        zoom=zoom,
        bbox_bucket=bbox_bucket,
        storage_backend=storage_backend,
        storage_key=storage_key,
        status="ready",
        metadata_extra=metadata_extra,
        last_hit_at=_now_utc(),
    )


async def get_raster_cache_status_index(
    session: AsyncSession,
    *,
    layer_ids: list[str],
    cache_kind: str,
    date_from: date,
    date_to: date,
    bbox_bucket: str | list[str] | None,
    zoom_levels: list[int] | None = None,
) -> dict[str, dict[str, str]]:
    if not layer_ids:
        return {}
    date_start = _date_to_datetime(date_from)
    date_end = _date_to_datetime(date_to + timedelta(days=1))
    query = select(RasterCacheEntry).where(
        RasterCacheEntry.cache_kind == cache_kind,
        RasterCacheEntry.layer_id.in_(layer_ids),
        RasterCacheEntry.display_date >= date_start,
        RasterCacheEntry.display_date < date_end,
    )
    if isinstance(bbox_bucket, list) and bbox_bucket:
        query = query.where(RasterCacheEntry.bbox_bucket.in_(bbox_bucket))
    elif bbox_bucket:
        query = query.where(RasterCacheEntry.bbox_bucket == bbox_bucket)
    if zoom_levels:
        query = query.where(RasterCacheEntry.zoom.in_([int(level) for level in zoom_levels]))
    result = await session.execute(query.order_by(desc(RasterCacheEntry.updated_at)))
    index: dict[str, dict[str, str]] = {}
    for row in result.scalars().all():
        if row.display_date is None:
            continue
        layer_index = index.setdefault(row.layer_id, {})
        observed_key = row.display_date.date().isoformat()
        if observed_key in layer_index:
            continue
        layer_index[observed_key] = row.status or "missing"
    return index


async def create_preload_run(
    session: AsyncSession,
    *,
    run_key: str,
    run_type: str,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    progress_total: int = 0,
    progress_done: int = 0,
    stage: str | None = None,
    status: str = "pending",
    details: dict[str, Any] | None = None,
) -> PreloadRun:
    row = PreloadRun(
        run_key=run_key,
        run_type=run_type,
        scope_type=scope_type,
        scope_ref=scope_ref,
        progress_total=progress_total,
        progress_done=progress_done,
        stage=stage,
        status=status,
        details=details or {},
    )
    session.add(row)
    await session.flush()
    return row


async def update_preload_run(
    session: AsyncSession,
    *,
    run_key: str,
    status: str | None = None,
    progress_total: int | None = None,
    progress_done: int | None = None,
    stage: str | None = None,
    details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> PreloadRun | None:
    result = await session.execute(select(PreloadRun).where(PreloadRun.run_key == run_key).limit(1))
    row = result.scalar_one_or_none()
    if row is None:
        return None
    if status is not None:
        row.status = status
    if progress_total is not None:
        row.progress_total = progress_total
    if progress_done is not None:
        row.progress_done = progress_done
    if stage is not None:
        row.stage = stage
    if details is not None:
        row.details = details
    if error_message is not None:
        row.error_message = error_message
    row.updated_at = _now_utc()
    await session.flush()
    return row


async def get_preload_run(session: AsyncSession, run_key: str) -> PreloadRun | None:
    result = await session.execute(select(PreloadRun).where(PreloadRun.run_key == run_key).limit(1))
    return result.scalar_one_or_none()


async def sweep_stale_preload_runs(
    session: AsyncSession,
    *,
    stale_minutes: int,
) -> int:
    """Mark preload_runs stuck in running/queued for more than `stale_minutes` as failed.

    Best-effort safety net so crashed workers, Windows sleeps, or process restarts
    do not leave zombie rows that forever look like "in progress". Returns the
    number of rows patched.
    """
    if stale_minutes <= 0:
        return 0
    cutoff = _now_utc() - timedelta(minutes=stale_minutes)
    stmt = (
        update(PreloadRun)
        .where(
            and_(
                PreloadRun.status.in_(("running", "queued")),
                PreloadRun.updated_at < cutoff,
            )
        )
        .values(
            status="failed",
            stage="failed",
            error_message=f"stale preload (>{stale_minutes}min without progress)",
            updated_at=_now_utc(),
        )
    )
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


def serialize_preload_run(row: PreloadRun | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "run_key": row.run_key,
        "run_type": row.run_type,
        "scope_type": row.scope_type,
        "scope_ref": row.scope_ref,
        "status": row.status,
        "progress_total": row.progress_total,
        "progress_done": row.progress_done,
        "stage": row.stage,
        "details": row.details or {},
        "error_message": row.error_message,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
