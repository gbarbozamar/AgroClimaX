from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import desc, select
from sqlalchemy.exc import OperationalError

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.materialized import PreloadRun, RasterCacheEntry
from app.services.analysis import get_timeline_context
from app.services.public_api import (
    OFFICIAL_MAP_OVERLAYS,
    TEMPORAL_LAYER_CONFIGS,
    TILE_MAX_ZOOM,
    TILE_MIN_ZOOM,
    _resolve_timeline_source_metadata,
    build_timeline_frame_manifest,
    fetch_tile_png,
    proxy_official_overlay_tile,
    resolve_temporal_layer_id,
)
from app.services.raster_products import materialize_viewport_raster_product
from app.services.raster_cache import (
    create_preload_run,
    get_preload_run,
    raster_cache_key,
    serialize_preload_run,
    update_preload_run,
    upsert_raster_cache_entry,
    viewport_bucket,
)
from app.services.tileserver_client import fetch_tileserver_tile


PRELOAD_TASKS: dict[str, asyncio.Task] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value: datetime | None) -> datetime:
    if value is None:
        return _now_utc()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _preload_task_liveness_window() -> timedelta:
    seconds = max(15, int(getattr(settings, "preload_task_liveness_seconds", 90) or 90))
    return timedelta(seconds=seconds)


def _is_preload_transient_status(status: str | None) -> bool:
    return str(status or "").strip().lower() in {"pending", "queued", "running"}


def _preload_run_signature(
    *,
    run_type: str,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str],
    official_layers: list[str],
    scope_type: str | None,
    scope_ref: str | None,
    timeline_scope: str,
    timeline_unit_id: str | None,
    timeline_department: str | None,
    date_from: date,
    date_to: date,
) -> str:
    viewport_key = viewport_bucket(bbox, zoom=zoom, width=width, height=height)
    return "::".join(
        [
            run_type,
            str(scope_type or "-"),
            str(scope_ref or "-"),
            str(timeline_scope or "-"),
            str(timeline_unit_id or "-"),
            str(timeline_department or "-"),
            viewport_key,
            ",".join(temporal_layers or []),
            ",".join(official_layers or []),
            date_from.isoformat(),
            date_to.isoformat(),
        ]
    )


def _mark_preload_row_state(
    row: PreloadRun,
    *,
    status: str,
    message: str | None = None,
    active_stage: str | None = None,
    residual_stage: str | None = None,
) -> None:
    details = dict(row.details or {})
    row.status = status
    row.updated_at = _now_utc()
    if message:
        row.error_message = message
    if active_stage is not None:
        details["active_stage"] = active_stage
    if residual_stage is not None:
        details["residual_stage"] = residual_stage
    row.details = details


async def _find_compatible_preload_run(
    session,
    *,
    run_signature: str,
    run_type: str,
    scope_type: str | None,
    scope_ref: str | None,
) -> PreloadRun | None:
    query = select(PreloadRun).where(PreloadRun.run_type == run_type)
    if scope_type is not None:
        query = query.where(PreloadRun.scope_type == scope_type)
    if scope_ref is not None:
        query = query.where(PreloadRun.scope_ref == scope_ref)
    result = await session.execute(query.order_by(desc(PreloadRun.updated_at)))
    rows = list(result.scalars().all())
    if not rows:
        return None

    now = _now_utc()
    stale_cutoff = now - _preload_task_liveness_window()
    match: PreloadRun | None = None
    dirty = False

    for row in rows:
        if not _is_preload_transient_status(row.status):
            continue
        details = row.details or {}
        has_local_task = row.run_key in PRELOAD_TASKS and not PRELOAD_TASKS[row.run_key].done()
        updated_at = _coerce_utc(row.updated_at or row.created_at or now)
        is_recent = updated_at >= stale_cutoff
        if str(details.get("run_signature") or "") == run_signature:
            if has_local_task or is_recent:
                match = row
                continue
            _mark_preload_row_state(
                row,
                status="stale",
                message="Preload run without active worker exceeded liveness window.",
                active_stage=details.get("active_stage") or row.stage,
                residual_stage=details.get("residual_stage"),
            )
            dirty = True
            continue
        if not has_local_task and updated_at < stale_cutoff:
            _mark_preload_row_state(
                row,
                status="stale",
                message="Preload run without active worker exceeded liveness window.",
                active_stage=details.get("active_stage") or row.stage,
                residual_stage=details.get("residual_stage"),
            )
            dirty = True

    if dirty:
        await session.flush()
    return match


def _set_active_stage(details: dict[str, Any], *, active_stage: str | None = None, residual_stage: str | None = None) -> dict[str, Any]:
    if active_stage is not None:
        details["active_stage"] = active_stage
    if residual_stage is not None:
        details["residual_stage"] = residual_stage
    return details


def _default_bbox() -> str:
    return f"{settings.aoi_bbox_west},{settings.aoi_bbox_south},{settings.aoi_bbox_east},{settings.aoi_bbox_north}"


def _default_temporal_prewarm_bbox() -> str:
    return settings.temporal_prewarm_bbox.strip() or _default_bbox()


def _is_sqlite_backend() -> bool:
    return "sqlite" in str(settings.database_url or "").lower()


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    if not isinstance(exc, OperationalError):
        return False
    message = str(exc).lower()
    return _is_sqlite_backend() and ("database is locked" in message or "database is busy" in message)


def _recommended_official_overlays() -> list[str]:
    return [overlay_id for overlay_id, config in OFFICIAL_MAP_OVERLAYS.items() if config.get("recommended")]


def _normalize_temporal_layers(layers: list[str] | None) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for layer in layers or []:
        internal = resolve_temporal_layer_id(layer)
        if internal and internal not in seen:
            resolved.append(internal)
            seen.add(internal)
    if not resolved:
        resolved = ["alerta_fusion"]
    return resolved


def _normalize_official_layers(layer_ids: list[str] | None) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for layer_id in layer_ids or []:
        if layer_id in OFFICIAL_MAP_OVERLAYS and layer_id not in seen:
            resolved.append(layer_id)
            seen.add(layer_id)
    if not resolved:
        return _recommended_official_overlays()
    return resolved


def _coerce_str_list(value: Any | None) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _parse_bbox(bbox: str | None) -> tuple[float, float, float, float]:
    raw = bbox or _default_bbox()
    parts = [part.strip() for part in raw.split(",")[:4]]
    west, south, east, north = [float(part) for part in parts]
    return west, south, east, north


def _lon_to_tile_x(lon: float, zoom: int) -> int:
    n = 2**zoom
    lon = max(-180.0, min(180.0, lon))
    return max(0, min(n - 1, int(math.floor((lon + 180.0) / 360.0 * n))))


def _lat_to_tile_y(lat: float, zoom: int) -> int:
    n = 2**zoom
    lat = max(-85.05112878, min(85.05112878, lat))
    lat_rad = math.radians(lat)
    tile_y = int(math.floor((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n))
    return max(0, min(n - 1, tile_y))


def _tile_coords_for_bbox(bbox: str | None, zoom: int) -> list[tuple[int, int]]:
    west, south, east, north = _parse_bbox(bbox)
    x_min = _lon_to_tile_x(west, zoom)
    x_max = _lon_to_tile_x(east, zoom)
    y_min = _lat_to_tile_y(north, zoom)
    y_max = _lat_to_tile_y(south, zoom)
    coords = [(x, y) for x in range(min(x_min, x_max), max(x_min, x_max) + 1) for y in range(min(y_min, y_max), max(y_min, y_max) + 1)]
    return _sample_tile_coords(coords, max_tiles=settings.preload_max_tiles_per_zoom)


def _sample_axis_values(values: list[int], max_count: int) -> list[int]:
    if len(values) <= max_count:
        return values
    if max_count <= 1:
        return [values[len(values) // 2]]
    step = (len(values) - 1) / float(max_count - 1)
    sampled: list[int] = []
    seen: set[int] = set()
    for index in range(max_count):
        candidate = values[int(round(index * step))]
        if candidate in seen:
            continue
        sampled.append(candidate)
        seen.add(candidate)
    if len(sampled) >= max_count:
        return sampled[:max_count]
    for candidate in values:
        if candidate in seen:
            continue
        sampled.append(candidate)
        if len(sampled) >= max_count:
            break
    return sampled


def _sample_tile_coords(coords: list[tuple[int, int]], *, max_tiles: int) -> list[tuple[int, int]]:
    max_tiles = max(int(max_tiles or 0), 1)
    if len(coords) <= max_tiles:
        return coords
    x_values = sorted({x for x, _ in coords})
    y_values = sorted({y for _, y in coords})
    sample_x_count = min(len(x_values), max(1, int(math.sqrt(max_tiles))))
    sample_y_count = min(len(y_values), max(1, math.ceil(max_tiles / max(sample_x_count, 1))))
    sampled_x = _sample_axis_values(x_values, sample_x_count)
    sampled_y = _sample_axis_values(y_values, sample_y_count)
    sampled = [(x, y) for x in sampled_x for y in sampled_y if (x, y) in coords]
    if len(sampled) >= max_tiles:
        return sampled[:max_tiles]
    sampled_set = set(sampled)
    for coord in coords:
        if coord in sampled_set:
            continue
        sampled.append(coord)
        if len(sampled) >= max_tiles:
            break
    return sampled


def _critical_tile_coords_for_bbox(bbox: str | None, zoom: int) -> list[tuple[int, int]]:
    west, south, east, north = _parse_bbox(bbox)
    x_min = _lon_to_tile_x(west, zoom)
    x_max = _lon_to_tile_x(east, zoom)
    y_min = _lat_to_tile_y(north, zoom)
    y_max = _lat_to_tile_y(south, zoom)
    coords = [(x, y) for x in range(min(x_min, x_max), max(x_min, x_max) + 1) for y in range(min(y_min, y_max), max(y_min, y_max) + 1)]
    return _sample_tile_coords(coords, max_tiles=settings.preload_critical_max_tiles_per_zoom)


def _zoom_levels(base_zoom: int | None) -> list[int]:
    resolved = max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, int(base_zoom or TILE_MIN_ZOOM)))
    adjacent = max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, resolved + max(settings.preload_adjacent_zoom_delta, 0)))
    zooms = [resolved]
    if adjacent not in zooms:
        zooms.append(adjacent)
    return zooms


def _neighbor_dates(target_date: date | None) -> list[date]:
    anchor = target_date or date.today()
    neighbors = []
    for offset in range(-max(settings.preload_neighbor_days, 0), max(settings.preload_neighbor_days, 0) + 1):
        neighbors.append(anchor + timedelta(days=offset))
    return sorted(set(neighbors))


def _serialize_stage(stage: str, total: int = 0) -> dict[str, Any]:
    return {"key": stage, "status": "pending", "done": 0, "total": total}


def _storage_backend_label() -> str:
    return "filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem"


async def _commit_run_update(
    run_key: str,
    *,
    progress_total: int | None = None,
    progress_done: int | None = None,
    stage: str | None = None,
    status: str | None = None,
    details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    try:
        async with AsyncSessionLocal() as session:
            await update_preload_run(
                session,
                run_key=run_key,
                progress_total=progress_total,
                progress_done=progress_done,
                stage=stage,
                status=status,
                details=details,
                error_message=error_message,
            )
            await session.commit()
    except OperationalError:
        return


async def _mark_temporal_cache_ready(
    *,
    layer_id: str,
    display_date: date,
    source_date: str | date,
    zoom: int,
    bbox: str | None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    bytes_size: int,
    frame_metadata: dict[str, Any] | None = None,
) -> None:
    bucket = viewport_bucket(bbox, zoom=zoom)
    frame_metadata = frame_metadata or {}
    try:
        async with AsyncSessionLocal() as session:
            cache_key = raster_cache_key(
                cache_kind="analytic_tile",
                layer_id=TEMPORAL_LAYER_CONFIGS[layer_id]["public_id"],
                display_date=display_date,
                source_date=source_date,
                zoom=zoom,
                bbox_bucket=bucket,
                scope_type=scope_type,
                scope_ref=scope_ref,
            )
            result = await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key).limit(1))
            existing_row = result.scalar_one_or_none()
            existing_metadata = dict(existing_row.metadata_extra) if existing_row and isinstance(existing_row.metadata_extra, dict) else {}
            if "visual_empty" in frame_metadata:
                visual_empty = bool(frame_metadata.get("visual_empty"))
            else:
                visual_empty = bool(existing_metadata.get("visual_empty"))
            visual_state = str(frame_metadata.get("visual_state") or existing_metadata.get("visual_state") or ("empty" if visual_empty else "ready"))
            status = "empty" if visual_empty else visual_state
            await upsert_raster_cache_entry(
                session,
                cache_key=cache_key,
                layer_id=TEMPORAL_LAYER_CONFIGS[layer_id]["public_id"],
                cache_kind="analytic_tile",
                scope_type=scope_type,
                scope_ref=scope_ref,
                display_date=display_date,
                source_date=source_date,
                zoom=zoom,
                bbox_bucket=bucket,
                storage_backend=_storage_backend_label(),
                storage_key=f"temporal/{layer_id}/{display_date.isoformat()}/{zoom}/{bucket}.png",
                status=status,
                bytes_size=bytes_size,
                metadata_extra={
                    **existing_metadata,
                    **frame_metadata,
                    "bbox": bbox,
                    "source_date": source_date,
                    "visual_empty": visual_empty,
                    "visual_state": visual_state,
                    "renderable_pixel_pct": frame_metadata.get("renderable_pixel_pct", existing_metadata.get("renderable_pixel_pct")),
                    "empty_reason": frame_metadata.get("empty_reason", existing_metadata.get("empty_reason")),
                },
                last_warmed_at=_now_utc(),
                last_hit_at=_now_utc(),
                expires_at=_now_utc() + timedelta(hours=settings.preload_run_ttl_hours),
            )
            await session.commit()
    except OperationalError as exc:
        if _is_sqlite_lock_error(exc):
            return
        raise


async def _mark_overlay_cache_ready(
    *,
    overlay_id: str,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    scope_type: str | None,
    scope_ref: str | None,
    bytes_size: int,
) -> None:
    bucket = viewport_bucket(bbox, zoom=zoom, width=width, height=height)
    try:
        async with AsyncSessionLocal() as session:
            await upsert_raster_cache_entry(
                session,
                cache_key=raster_cache_key(
                    cache_kind="official_overlay_viewport",
                    layer_id=overlay_id,
                    zoom=zoom,
                    bbox_bucket=bucket,
                    scope_type=scope_type,
                    scope_ref=scope_ref,
                ),
                layer_id=overlay_id,
                cache_kind="official_overlay_viewport",
                scope_type=scope_type,
                scope_ref=scope_ref,
                zoom=zoom,
                bbox_bucket=bucket,
                storage_backend=_storage_backend_label(),
                storage_key=f"official/{overlay_id}/{bucket}",
                status="ready",
                bytes_size=bytes_size,
                metadata_extra={"bbox": bbox, "width": width, "height": height},
                last_warmed_at=_now_utc(),
                last_hit_at=_now_utc(),
                expires_at=_now_utc() + timedelta(hours=settings.preload_run_ttl_hours),
            )
            await session.commit()
    except OperationalError as exc:
        if _is_sqlite_lock_error(exc):
            return
        raise


async def _mark_context_cache_ready(
    *,
    scope: str,
    unit_id: str | None,
    department: str | None,
    target_date: date,
    history_days: int,
    scope_ref: str | None,
) -> None:
    bucket = f"context:{scope}:{unit_id or department or 'uruguay'}:{history_days}"
    try:
        async with AsyncSessionLocal() as session:
            await upsert_raster_cache_entry(
                session,
                cache_key=raster_cache_key(
                    cache_kind="timeline_context",
                    layer_id="timeline_context",
                    display_date=target_date,
                    scope_type=scope,
                    scope_ref=scope_ref or unit_id or department or "Uruguay",
                    bbox_bucket=bucket,
                ),
                layer_id="timeline_context",
                cache_kind="timeline_context",
                scope_type=scope,
                scope_ref=scope_ref or unit_id or department or "Uruguay",
                display_date=target_date,
                bbox_bucket=bucket,
                storage_backend="database",
                storage_key=bucket,
                status="ready",
                bytes_size=1,
                metadata_extra={"history_days": history_days, "unit_id": unit_id, "department": department},
                last_warmed_at=_now_utc(),
                last_hit_at=_now_utc(),
                expires_at=_now_utc() + timedelta(hours=settings.preload_run_ttl_hours),
            )
            await session.commit()
    except OperationalError as exc:
        if _is_sqlite_lock_error(exc):
            return
        raise


async def _mark_manifest_cache_ready(
    *,
    layer_ids: list[str],
    bbox: str | None,
    zoom: int | None,
    date_from: date,
    date_to: date,
    scope_ref: str | None,
) -> None:
    bucket = viewport_bucket(bbox, zoom=zoom)
    try:
        async with AsyncSessionLocal() as session:
            await upsert_raster_cache_entry(
                session,
                cache_key=raster_cache_key(
                    cache_kind="timeline_manifest",
                    layer_id=",".join(sorted(layer_ids)),
                    display_date=date_to,
                    zoom=zoom,
                    bbox_bucket=bucket,
                    scope_type="timeline_manifest",
                    scope_ref=scope_ref,
                ),
                layer_id=",".join(sorted(layer_ids)),
                cache_kind="timeline_manifest",
                scope_type="timeline_manifest",
                scope_ref=scope_ref,
                display_date=date_to,
                zoom=zoom,
                bbox_bucket=bucket,
                storage_backend="database",
                storage_key=f"manifest:{bucket}:{date_from.isoformat()}:{date_to.isoformat()}",
                status="ready",
                bytes_size=1,
                metadata_extra={"layers": layer_ids, "date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
                last_warmed_at=_now_utc(),
                last_hit_at=_now_utc(),
                expires_at=_now_utc() + timedelta(hours=settings.preload_run_ttl_hours),
            )
            await session.commit()
    except OperationalError as exc:
        if _is_sqlite_lock_error(exc):
            return
        raise


def _build_initial_details(
    *,
    temporal_layers: list[str],
    official_layers: list[str],
    target_dates: list[date],
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
) -> dict[str, Any]:
    return {
        "critical_ready": False,
        "critical_stage_key": "analytic_neighbors",
        "active_stage": "timeline_manifest",
        "residual_stage": None,
        "stages": {
            "timeline_manifest": _serialize_stage("timeline_manifest", total=1),
            "timeline_context": _serialize_stage("timeline_context", total=len(target_dates)),
            "analytic_neighbors": _serialize_stage("analytic_neighbors"),
            "official_overlays": _serialize_stage("official_overlays", total=len(official_layers)),
        },
        "target_dates": [item.isoformat() for item in target_dates],
        "temporal_layers": [TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in temporal_layers],
        "official_layers": official_layers,
        "bbox": bbox,
        "zoom": zoom,
        "width": width,
        "height": height,
    }


def _set_stage_status(details: dict[str, Any], stage_key: str, *, status: str | None = None, done: int | None = None, total: int | None = None) -> dict[str, Any]:
    stages = details.setdefault("stages", {})
    stage = stages.setdefault(stage_key, _serialize_stage(stage_key))
    if status is not None:
        stage["status"] = status
    if done is not None:
        stage["done"] = done
    if total is not None:
        stage["total"] = total
    stages[stage_key] = stage
    return details


async def _execute_preload_run(
    run_key: str,
    *,
    run_type: str,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str],
    official_layers: list[str],
    scope_type: str | None,
    scope_ref: str | None,
    timeline_scope: str,
    timeline_unit_id: str | None,
    timeline_department: str | None,
    target_date: date,
    history_days: int,
    date_from: date,
    date_to: date,
) -> None:
    target_dates = _neighbor_dates(target_date) if run_type != "timeline_window" else [
        date_from + timedelta(days=offset) for offset in range((date_to - date_from).days + 1)
    ]
    zoom_levels = _zoom_levels(zoom)
    critical_tile_tasks = sum(len(_critical_tile_coords_for_bbox(bbox, level)) * len(target_dates) * len(temporal_layers) for level in [zoom_levels[0]])
    overlay_tasks = len(official_layers) if settings.preload_enabled else 0
    progress_total = 1 + len(target_dates) + critical_tile_tasks + overlay_tasks
    if len(zoom_levels) > 1:
        progress_total += len(_tile_coords_for_bbox(bbox, zoom_levels[1])) * len(target_dates) * len(temporal_layers)

    details = _build_initial_details(
        temporal_layers=temporal_layers,
        official_layers=official_layers,
        target_dates=target_dates,
        bbox=bbox,
        zoom=zoom_levels[0],
        width=width,
        height=height,
    )
    details["timeline_scope"] = timeline_scope
    details["timeline_unit_id"] = timeline_unit_id
    details["timeline_department"] = timeline_department

    progress_done = 0
    spatial_scope_ref = scope_ref or timeline_unit_id or timeline_department or timeline_scope
    source_date_cache: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    try:
        await _commit_run_update(
            run_key,
            status="running",
            stage="timeline_manifest",
            progress_total=progress_total,
            progress_done=progress_done,
            details=details,
        )

        await build_timeline_frame_manifest(
            layers=[TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in temporal_layers],
            date_from=date_from,
            date_to=date_to,
            bbox=bbox,
            zoom=zoom_levels[0],
            scope=timeline_scope,
            unit_id=timeline_unit_id,
            department=timeline_department,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )
        await _mark_manifest_cache_ready(
            layer_ids=[TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in temporal_layers],
            bbox=bbox,
            zoom=zoom_levels[0],
            date_from=date_from,
            date_to=date_to,
            scope_ref=scope_ref,
        )
        progress_done += 1
        details = _set_stage_status(details, "timeline_manifest", status="done", done=1, total=1)
        details = _set_active_stage(details, active_stage="timeline_context", residual_stage=None)
        await _commit_run_update(run_key, stage="timeline_context", progress_done=progress_done, details=details)

        if timeline_scope in {"nacional", "departamento", "unidad"}:
            context_done = 0
            for day in target_dates:
                async with AsyncSessionLocal() as session:
                    await get_timeline_context(
                        session,
                        scope=timeline_scope,
                        unit_id=timeline_unit_id,
                        department=timeline_department,
                        target_date=day,
                        history_days=history_days,
                    )
                await _mark_context_cache_ready(
                    scope=timeline_scope,
                    unit_id=timeline_unit_id,
                    department=timeline_department,
                    target_date=day,
                    history_days=history_days,
                    scope_ref=scope_ref,
                )
                context_done += 1
                progress_done += 1
                details = _set_stage_status(details, "timeline_context", status="running", done=context_done, total=len(target_dates))
                await _commit_run_update(run_key, progress_done=progress_done, details=details)
            details = _set_stage_status(details, "timeline_context", status="done", done=len(target_dates), total=len(target_dates))
            details = _set_active_stage(details, active_stage="analytic_neighbors", residual_stage=None)
            await _commit_run_update(run_key, stage="analytic_neighbors", progress_done=progress_done, details=details)

        critical_done = 0
        critical_total = critical_tile_tasks
        details = _set_stage_status(details, "analytic_neighbors", status="running", done=critical_done, total=critical_total)
        await _commit_run_update(run_key, details=details)
        for layer in temporal_layers:
            for day in target_dates:
                current_zoom = zoom_levels[0]
                current_bbox_bucket = viewport_bucket(bbox, zoom=current_zoom)
                critical_tile_coords = _critical_tile_coords_for_bbox(bbox, current_zoom)
                full_tile_coords = _tile_coords_for_bbox(bbox, current_zoom)
                source_cache_key = (layer, day.isoformat(), current_bbox_bucket, spatial_scope_ref)
                source_metadata = source_date_cache.get(source_cache_key)
                if source_metadata is None:
                    source_metadata = await _resolve_timeline_source_metadata(
                        layer,
                        day,
                        bbox_bucket=current_bbox_bucket,
                        bbox=bbox,
                        zoom=current_zoom,
                        scope=timeline_scope,
                        unit_id=timeline_unit_id,
                        department=timeline_department,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                    )
                    source_date_cache[source_cache_key] = source_metadata
                source_date_value = str(source_metadata.get("primary_source_date") or day.isoformat())
                for x, y in critical_tile_coords:
                    content = await fetch_tile_png(
                        layer,
                        current_zoom,
                        x,
                        y,
                        target_date=day,
                        frame_role="primary",
                        scope=timeline_scope,
                        unit_id=timeline_unit_id,
                        department=timeline_department,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                        viewport_bbox=bbox,
                        viewport_zoom=current_zoom,
                    )
                    await _mark_temporal_cache_ready(
                        layer_id=layer,
                        display_date=day,
                        source_date=source_date_value,
                        zoom=current_zoom,
                        bbox=bbox,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                        bytes_size=len(content or b""),
                        frame_metadata=source_metadata,
                    )
                    critical_done += 1
                    progress_done += 1
                    details = _set_stage_status(details, "analytic_neighbors", status="running", done=critical_done, total=critical_total)
                    await _commit_run_update(run_key, progress_done=progress_done, details=details)
                if len(critical_tile_coords) == len(full_tile_coords):
                    await materialize_viewport_raster_product(
                        layer_id=layer,
                        display_date=day,
                        source_date=source_date_value,
                        bbox=bbox,
                        zoom=current_zoom,
                        bbox_bucket=current_bbox_bucket,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                        metadata_extra={
                            **source_metadata,
                            "coverage_origin": str(source_metadata.get("coverage_origin") or "preload_viewport_product"),
                            "resolved_from_cache": False,
                            "preload_sampling_mode": "full",
                        },
                    )
        details = _set_stage_status(details, "analytic_neighbors", status="done", done=critical_done, total=critical_total)
        details["critical_ready"] = True
        details = _set_active_stage(
            details,
            active_stage="critical_ready",
            residual_stage="official_overlays" if official_layers else None,
        )
        await _commit_run_update(run_key, stage="official_overlays", progress_done=progress_done, details=details)

        if len(zoom_levels) > 1:
            for layer in temporal_layers:
                for day in target_dates:
                    current_zoom = zoom_levels[1]
                    current_bbox_bucket = viewport_bucket(bbox, zoom=current_zoom)
                    full_tile_coords = _tile_coords_for_bbox(bbox, current_zoom)
                    source_cache_key = (layer, day.isoformat(), current_bbox_bucket, spatial_scope_ref)
                    source_metadata = source_date_cache.get(source_cache_key)
                    if source_metadata is None:
                        source_metadata = await _resolve_timeline_source_metadata(
                            layer,
                            day,
                            bbox_bucket=current_bbox_bucket,
                            bbox=bbox,
                            zoom=current_zoom,
                            scope=timeline_scope,
                            unit_id=timeline_unit_id,
                            department=timeline_department,
                            scope_type=scope_type,
                            scope_ref=scope_ref,
                        )
                        source_date_cache[source_cache_key] = source_metadata
                    source_date_value = str(source_metadata.get("primary_source_date") or day.isoformat())
                    for x, y in full_tile_coords:
                        content = await fetch_tile_png(
                            layer,
                            current_zoom,
                            x,
                            y,
                            target_date=day,
                            frame_role="primary",
                            scope=timeline_scope,
                            unit_id=timeline_unit_id,
                            department=timeline_department,
                            scope_type=scope_type,
                            scope_ref=scope_ref,
                            viewport_bbox=bbox,
                            viewport_zoom=current_zoom,
                        )
                        await _mark_temporal_cache_ready(
                            layer_id=layer,
                            display_date=day,
                            source_date=source_date_value,
                            zoom=current_zoom,
                            bbox=bbox,
                            scope_type=scope_type,
                            scope_ref=scope_ref,
                            bytes_size=len(content or b""),
                            frame_metadata=source_metadata,
                        )
                        progress_done += 1
                        await _commit_run_update(run_key, progress_done=progress_done, details=details)
                    await materialize_viewport_raster_product(
                        layer_id=layer,
                        display_date=day,
                        source_date=source_date_value,
                        bbox=bbox,
                        zoom=current_zoom,
                        bbox_bucket=current_bbox_bucket,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                            metadata_extra={
                                **source_metadata,
                                "coverage_origin": str(source_metadata.get("coverage_origin") or "preload_viewport_product"),
                                "resolved_from_cache": False,
                                "preload_sampling_mode": "full",
                            },
                        )

        overlay_done = 0
        details = _set_stage_status(details, "official_overlays", status="running", done=overlay_done, total=len(official_layers))
        details = _set_active_stage(details, active_stage="critical_ready", residual_stage="official_overlays")
        await _commit_run_update(run_key, details=details)
        if official_layers:
            overlay_semaphore = asyncio.Semaphore(max(1, int(settings.preload_official_overlay_parallelism or 1)))
            overlay_timeout = max(5.0, float(settings.preload_official_overlay_timeout_seconds or 20))

            async def _warm_single_overlay(overlay_id: str) -> tuple[str, bytes | None]:
                try:
                    async with overlay_semaphore:
                        content, _content_type = await asyncio.wait_for(
                            proxy_official_overlay_tile(
                                overlay_id,
                                {
                                    "bbox": bbox or _default_bbox(),
                                    "bboxSR": "4326",
                                    "imageSR": "4326",
                                    "width": width,
                                    "height": height,
                                    "format": "image/png",
                                    "transparent": "true",
                                },
                            ),
                            timeout=overlay_timeout,
                        )
                    await _mark_overlay_cache_ready(
                        overlay_id=overlay_id,
                        bbox=bbox,
                        zoom=zoom_levels[0],
                        width=width,
                        height=height,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                        bytes_size=len(content or b""),
                    )
                    return overlay_id, content
                except Exception:
                    return overlay_id, None

            overlay_tasks_iter = [_warm_single_overlay(overlay_id) for overlay_id in official_layers]
            for task in asyncio.as_completed(overlay_tasks_iter):
                _overlay_id, _content = await task
                overlay_done += 1
                progress_done += 1
                details = _set_stage_status(details, "official_overlays", status="running", done=overlay_done, total=len(official_layers))
                await _commit_run_update(run_key, progress_done=progress_done, details=details)
        details = _set_stage_status(details, "official_overlays", status="done", done=overlay_done, total=len(official_layers))
        details = _set_active_stage(details, active_stage="done", residual_stage=None)

        await _commit_run_update(run_key, status="success", stage="done", progress_done=progress_total, details=details)
    except Exception as exc:
        details["critical_ready"] = bool(details.get("critical_ready"))
        details = _set_active_stage(details, active_stage="failed", residual_stage=details.get("residual_stage"))
        await _commit_run_update(run_key, status="failed", stage="failed", progress_done=progress_done, details=details, error_message=str(exc))
    finally:
        PRELOAD_TASKS.pop(run_key, None)


def _register_background_task(run_key: str, coro) -> None:
    task = asyncio.create_task(coro)
    PRELOAD_TASKS[run_key] = task

    def _cleanup(_task: asyncio.Task) -> None:
        try:
            if not _task.cancelled():
                _task.exception()
        except Exception:
            pass
        PRELOAD_TASKS.pop(run_key, None)

    task.add_done_callback(_cleanup)


async def _create_and_schedule_run(
    *,
    run_type: str,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str] | None,
    official_layers: list[str] | None,
    preload_profile: str | None = None,
    scope_type: str | None,
    scope_ref: str | None,
    timeline_scope: str,
    timeline_unit_id: str | None,
    timeline_department: str | None,
    target_date: date | None,
    history_days: int,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    if not settings.preload_enabled:
        return {"status": "disabled"}
    resolved_target_date = target_date or date.today()
    resolved_temporal_layers = _normalize_temporal_layers(temporal_layers)
    resolved_official_layers = _normalize_official_layers(official_layers)
    profile = str(preload_profile or "").strip().lower()
    if profile in {"field_viewer", "viewer"}:
        resolved_official_layers = []
    resolved_date_to = date_to or resolved_target_date
    resolved_date_from = date_from or (resolved_target_date - timedelta(days=settings.preload_neighbor_days))
    run_signature = _preload_run_signature(
        run_type=run_type,
        bbox=bbox,
        zoom=zoom,
        width=width,
        height=height,
        temporal_layers=resolved_temporal_layers,
        official_layers=resolved_official_layers,
        scope_type=scope_type,
        scope_ref=scope_ref,
        timeline_scope=timeline_scope,
        timeline_unit_id=timeline_unit_id,
        timeline_department=timeline_department,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
    )
    run_key = uuid4().hex

    try:
        async with AsyncSessionLocal() as session:
            existing = await _find_compatible_preload_run(
                session,
                run_signature=run_signature,
                run_type=run_type,
                scope_type=scope_type,
                scope_ref=scope_ref,
            )
            if existing is not None:
                await session.commit()
                payload = serialize_preload_run(existing) or {}
                payload["critical_ready"] = bool((payload.get("details") or {}).get("critical_ready"))
                payload["active_stage"] = (payload.get("details") or {}).get("active_stage") or payload.get("stage")
                payload["residual_stage"] = (payload.get("details") or {}).get("residual_stage")
                return payload
            row = await create_preload_run(
                session,
                run_key=run_key,
                run_type=run_type,
                scope_type=scope_type,
                scope_ref=scope_ref,
                status="queued",
                stage="queued",
                details={
                    "critical_ready": False,
                    "preload_profile": profile or None,
                    "temporal_layers": [TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in resolved_temporal_layers],
                    "official_layers": resolved_official_layers,
                    "bbox": bbox,
                    "zoom": zoom,
                    "run_signature": run_signature,
                    "active_stage": "queued",
                    "residual_stage": None,
                },
            )
            await session.commit()
            payload = serialize_preload_run(row) or {}
    except OperationalError:
        return {
            "run_key": run_key,
            "status": "busy",
            "stage": "queued",
            "critical_ready": False,
              "details": {
                  "critical_ready": False,
                  "preload_profile": profile or None,
                  "reason": "database_locked",
                  "temporal_layers": [TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in resolved_temporal_layers],
                  "official_layers": resolved_official_layers,
                  "bbox": bbox,
                "zoom": zoom,
                "run_signature": run_signature,
                "active_stage": "queued",
                "residual_stage": None,
            },
        }

    _register_background_task(
        run_key,
        _execute_preload_run(
            run_key,
            run_type=run_type,
            bbox=bbox,
            zoom=zoom,
            width=width,
            height=height,
            temporal_layers=resolved_temporal_layers,
            official_layers=resolved_official_layers,
            scope_type=scope_type,
            scope_ref=scope_ref,
            timeline_scope=timeline_scope,
            timeline_unit_id=timeline_unit_id,
            timeline_department=timeline_department,
            target_date=resolved_target_date,
            history_days=history_days,
            date_from=resolved_date_from,
            date_to=resolved_date_to,
        ),
    )
    payload["status"] = "queued"
    payload["critical_ready"] = False
    payload["active_stage"] = "queued"
    payload["residual_stage"] = None
    return payload


async def start_startup_preload(
    *,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str] | None,
    official_layers: list[str] | None,
    preload_profile: str | None = None,
    scope_type: str | None,
    scope_ref: str | None,
    timeline_scope: str,
    timeline_unit_id: str | None,
    timeline_department: str | None,
    target_date: date | None = None,
    history_days: int = 30,
) -> dict[str, Any]:
    return await _create_and_schedule_run(
        run_type="startup",
        bbox=bbox,
        zoom=zoom,
        width=width,
        height=height,
        temporal_layers=temporal_layers,
        official_layers=official_layers,
        preload_profile=preload_profile,
        scope_type=scope_type,
        scope_ref=scope_ref,
        timeline_scope=timeline_scope,
        timeline_unit_id=timeline_unit_id,
        timeline_department=timeline_department,
        target_date=target_date,
        history_days=history_days,
    )


async def start_viewport_preload(
    *,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str] | None,
    official_layers: list[str] | None,
    preload_profile: str | None = None,
    scope_type: str | None,
    scope_ref: str | None,
    timeline_scope: str,
    timeline_unit_id: str | None,
    timeline_department: str | None,
    target_date: date | None = None,
    history_days: int = 30,
) -> dict[str, Any]:
    return await _create_and_schedule_run(
        run_type="field_focus" if scope_type in {"field", "paddock"} else "viewport",
        bbox=bbox,
        zoom=zoom,
        width=width,
        height=height,
        temporal_layers=temporal_layers,
        official_layers=official_layers,
        preload_profile=preload_profile,
        scope_type=scope_type,
        scope_ref=scope_ref,
        timeline_scope=timeline_scope,
        timeline_unit_id=timeline_unit_id,
        timeline_department=timeline_department,
        target_date=target_date,
        history_days=history_days,
    )


async def start_timeline_window_preload(
    *,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str] | None,
    scope_type: str | None,
    scope_ref: str | None,
    timeline_scope: str,
    timeline_unit_id: str | None,
    timeline_department: str | None,
    preload_profile: str | None = None,
    date_from: date,
    date_to: date,
    history_days: int = 30,
) -> dict[str, Any]:
    return await _create_and_schedule_run(
        run_type="timeline_window",
        bbox=bbox,
        zoom=zoom,
        width=width,
        height=height,
        temporal_layers=temporal_layers,
        official_layers=[],
        preload_profile=preload_profile,
        scope_type=scope_type,
        scope_ref=scope_ref,
        timeline_scope=timeline_scope,
        timeline_unit_id=timeline_unit_id,
        timeline_department=timeline_department,
        target_date=date_to,
        history_days=history_days,
        date_from=date_from,
        date_to=date_to,
    )


async def schedule_default_temporal_preload() -> dict[str, Any]:
    scope_type = str(settings.temporal_prewarm_scope_type or "nacional").strip().lower()
    scope_ref = settings.temporal_prewarm_scope_ref.strip() or None
    timeline_scope = "nacional"
    timeline_unit_id = settings.temporal_prewarm_unit_id.strip() or None
    timeline_department = settings.temporal_prewarm_department.strip() or None

    if scope_type in {"departamento", "department"}:
        scope_type = "departamento"
        timeline_scope = "departamento"
        scope_ref = scope_ref or timeline_department or "departamento"
    elif scope_type in {"unidad", "unit", "field", "paddock"} and timeline_unit_id:
        scope_type = scope_type if scope_type in {"field", "paddock"} else "unidad"
        timeline_scope = "unidad"
        scope_ref = scope_ref or timeline_unit_id
    else:
        scope_type = "nacional"
        timeline_scope = "nacional"
        scope_ref = scope_ref or "Uruguay"

    return await start_startup_preload(
        bbox=_default_temporal_prewarm_bbox(),
        zoom=max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, int(settings.temporal_prewarm_zoom))),
        width=max(256, int(settings.temporal_prewarm_width)),
        height=max(256, int(settings.temporal_prewarm_height)),
        temporal_layers=_coerce_str_list(getattr(settings, "temporal_prewarm_temporal_layers", None)) or ["alerta", "rgb", "ndmi"],
        official_layers=[],
        scope_type=scope_type,
        scope_ref=scope_ref,
        timeline_scope=timeline_scope,
        timeline_unit_id=timeline_unit_id,
        timeline_department=timeline_department,
        target_date=date.today(),
        history_days=max(1, int(settings.temporal_prewarm_history_days)),
    )


async def warm_tileserver_temporal_tiles(
    *,
    layers: list[str] | None,
    date_from: date,
    date_to: date,
    bbox: str | None,
    zoom: int | None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    max_tiles_per_zoom: int | None = None,
    critical_only: bool = True,
) -> dict[str, Any]:
    resolved_layers = _normalize_temporal_layers(layers or [])
    if not resolved_layers:
        return {"status": "empty", "tiles": 0, "ready": 0, "empty_tiles": 0, "missing": 0}
    if date_to < date_from:
        raise ValueError("date_to no puede ser anterior a date_from")

    zoom_levels = _zoom_levels(zoom)
    if max_tiles_per_zoom is not None:
        max_tiles_per_zoom = max(int(max_tiles_per_zoom), 1)
    tiles_total = 0
    ready_tiles = 0
    empty_tiles = 0
    missing_tiles = 0

    current = date_from
    while current <= date_to:
        for layer_id in resolved_layers:
            for level in zoom_levels:
                coords = _critical_tile_coords_for_bbox(bbox, level) if critical_only else _tile_coords_for_bbox(bbox, level)
                if max_tiles_per_zoom is not None and len(coords) > max_tiles_per_zoom:
                    coords = coords[:max_tiles_per_zoom]
                for x, y in coords:
                    tiles_total += 1
                    try:
                        content, metadata = await fetch_tileserver_tile(
                            layer_id=layer_id,
                            display_date=current,
                            z=level,
                            x=x,
                            y=y,
                            unit_id=unit_id,
                            department=department,
                            scope_type=scope_type,
                            scope_ref=scope_ref,
                        )
                    except Exception:
                        content, metadata = None, None
                    if metadata is None:
                        missing_tiles += 1
                        continue
                    if metadata.get("visual_empty"):
                        empty_tiles += 1
                    else:
                        ready_tiles += 1
                    await _mark_temporal_cache_ready(
                        layer_id=layer_id,
                        display_date=current,
                        source_date=str(metadata.get("resolved_source_date") or current.isoformat()),
                        zoom=level,
                        bbox=bbox,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                        bytes_size=len(content or b""),
                        frame_metadata=metadata,
                    )
        current += timedelta(days=1)

    return {
        "status": "success",
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "layers": [TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in resolved_layers],
        "zoom_levels": zoom_levels,
        "tiles": tiles_total,
        "ready": ready_tiles,
        "empty_tiles": empty_tiles,
        "missing": missing_tiles,
    }


async def get_preload_status(run_key: str) -> dict[str, Any]:
    async with AsyncSessionLocal() as session:
        row = await get_preload_run(session, run_key)
        if row is not None and _is_preload_transient_status(row.status):
            has_local_task = run_key in PRELOAD_TASKS and not PRELOAD_TASKS[run_key].done()
            updated_at = _coerce_utc(row.updated_at or row.created_at or _now_utc())
            if not has_local_task and updated_at < (_now_utc() - _preload_task_liveness_window()):
                _mark_preload_row_state(
                    row,
                    status="stale",
                    message="Preload run without active worker exceeded liveness window.",
                    active_stage=(row.details or {}).get("active_stage") or row.stage,
                    residual_stage=(row.details or {}).get("residual_stage"),
                )
                await session.commit()
        payload = serialize_preload_run(row) or {"run_key": run_key, "status": "missing"}
    has_local_task = run_key in PRELOAD_TASKS and not PRELOAD_TASKS[run_key].done()
    payload["task_state"] = "running" if has_local_task else payload.get("status", "missing")
    details = payload.get("details") or {}
    payload["critical_ready"] = bool(details.get("critical_ready"))
    payload["active_stage"] = details.get("active_stage") or payload.get("stage")
    payload["residual_stage"] = details.get("residual_stage")
    return payload
