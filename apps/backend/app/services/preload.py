from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from app.core.config import settings
from app.db.session import AsyncSessionLocal
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
from app.services.raster_cache import (
    create_preload_run,
    get_preload_run,
    raster_cache_key,
    serialize_preload_run,
    update_preload_run,
    upsert_raster_cache_entry,
    viewport_bucket,
)


PRELOAD_TASKS: dict[str, asyncio.Task] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _default_bbox() -> str:
    return f"{settings.aoi_bbox_west},{settings.aoi_bbox_south},{settings.aoi_bbox_east},{settings.aoi_bbox_north}"


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
    if len(coords) <= settings.preload_max_tiles_per_zoom:
        return coords
    return coords[: settings.preload_max_tiles_per_zoom]


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


async def _mark_temporal_cache_ready(
    *,
    layer_id: str,
    display_date: date,
    source_date: str | date,
    zoom: int,
    bbox: str | None,
    bytes_size: int,
) -> None:
    bucket = viewport_bucket(bbox, zoom=zoom)
    async with AsyncSessionLocal() as session:
        await upsert_raster_cache_entry(
            session,
            cache_key=raster_cache_key(
                cache_kind="analytic_tile",
                layer_id=TEMPORAL_LAYER_CONFIGS[layer_id]["public_id"],
                display_date=display_date,
                source_date=source_date,
                zoom=zoom,
                bbox_bucket=bucket,
            ),
            layer_id=TEMPORAL_LAYER_CONFIGS[layer_id]["public_id"],
            cache_kind="analytic_tile",
            display_date=display_date,
            source_date=source_date,
            zoom=zoom,
            bbox_bucket=bucket,
            storage_backend=_storage_backend_label(),
            storage_key=f"temporal/{layer_id}/{display_date.isoformat()}/{zoom}/{bucket}.png",
            status="ready",
            bytes_size=bytes_size,
            metadata_extra={"bbox": bbox, "source_date": source_date},
            last_warmed_at=_now_utc(),
            last_hit_at=_now_utc(),
            expires_at=_now_utc() + timedelta(hours=settings.preload_run_ttl_hours),
        )
        await session.commit()


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
    critical_tile_tasks = sum(len(_tile_coords_for_bbox(bbox, level)) * len(target_dates) * len(temporal_layers) for level in [zoom_levels[0]])
    overlay_tasks = len(official_layers) if settings.preload_enabled else 0
    progress_total = 1 + len(target_dates) + critical_tile_tasks + overlay_tasks
    if len(zoom_levels) > 1:
        progress_total += sum(len(_tile_coords_for_bbox(bbox, zoom_levels[1])) * len(target_dates) * len(temporal_layers))

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
    source_date_cache: dict[tuple[str, str], str] = {}
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
            await _commit_run_update(run_key, stage="analytic_neighbors", progress_done=progress_done, details=details)

        critical_done = 0
        critical_total = critical_tile_tasks
        details = _set_stage_status(details, "analytic_neighbors", status="running", done=critical_done, total=critical_total)
        await _commit_run_update(run_key, details=details)
        for layer in temporal_layers:
            for day in target_dates:
                source_date_value = source_date_cache.get((layer, day.isoformat()))
                if source_date_value is None:
                    source_metadata = await _resolve_timeline_source_metadata(layer, day)
                    source_date_value = str(source_metadata.get("primary_source_date") or day.isoformat())
                    source_date_cache[(layer, day.isoformat())] = source_date_value
                for current_zoom in [zoom_levels[0]]:
                    for x, y in _tile_coords_for_bbox(bbox, current_zoom):
                        content = await fetch_tile_png(layer, current_zoom, x, y, target_date=day, frame_role="primary")
                        await _mark_temporal_cache_ready(
                            layer_id=layer,
                            display_date=day,
                            source_date=source_date_value,
                            zoom=current_zoom,
                            bbox=bbox,
                            bytes_size=len(content or b""),
                        )
                        critical_done += 1
                        progress_done += 1
                        details = _set_stage_status(details, "analytic_neighbors", status="running", done=critical_done, total=critical_total)
                        await _commit_run_update(run_key, progress_done=progress_done, details=details)
        details = _set_stage_status(details, "analytic_neighbors", status="done", done=critical_done, total=critical_total)
        details["critical_ready"] = True
        await _commit_run_update(run_key, stage="official_overlays", progress_done=progress_done, details=details)

        if len(zoom_levels) > 1:
            for layer in temporal_layers:
                for day in target_dates:
                    source_date_value = source_date_cache.get((layer, day.isoformat()))
                    if source_date_value is None:
                        source_metadata = await _resolve_timeline_source_metadata(layer, day)
                        source_date_value = str(source_metadata.get("primary_source_date") or day.isoformat())
                        source_date_cache[(layer, day.isoformat())] = source_date_value
                    for x, y in _tile_coords_for_bbox(bbox, zoom_levels[1]):
                        content = await fetch_tile_png(layer, zoom_levels[1], x, y, target_date=day, frame_role="primary")
                        await _mark_temporal_cache_ready(
                            layer_id=layer,
                            display_date=day,
                            source_date=source_date_value,
                            zoom=zoom_levels[1],
                            bbox=bbox,
                            bytes_size=len(content or b""),
                        )
                        progress_done += 1
                        await _commit_run_update(run_key, progress_done=progress_done, details=details)

        overlay_done = 0
        details = _set_stage_status(details, "official_overlays", status="running", done=overlay_done, total=len(official_layers))
        await _commit_run_update(run_key, details=details)
        for overlay_id in official_layers:
            content, _content_type = await proxy_official_overlay_tile(
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
            overlay_done += 1
            progress_done += 1
            details = _set_stage_status(details, "official_overlays", status="running", done=overlay_done, total=len(official_layers))
            await _commit_run_update(run_key, progress_done=progress_done, details=details)
        details = _set_stage_status(details, "official_overlays", status="done", done=overlay_done, total=len(official_layers))

        await _commit_run_update(run_key, status="success", stage="done", progress_done=progress_total, details=details)
    except Exception as exc:
        details["critical_ready"] = bool(details.get("critical_ready"))
        await _commit_run_update(run_key, status="failed", stage="failed", progress_done=progress_done, details=details, error_message=str(exc))
    finally:
        PRELOAD_TASKS.pop(run_key, None)


def _register_background_task(run_key: str, coro) -> None:
    task = asyncio.create_task(coro)
    PRELOAD_TASKS[run_key] = task

    def _cleanup(_task: asyncio.Task) -> None:
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
    resolved_date_to = date_to or resolved_target_date
    resolved_date_from = date_from or (resolved_target_date - timedelta(days=settings.preload_neighbor_days))
    run_key = uuid4().hex

    async with AsyncSessionLocal() as session:
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
                "temporal_layers": [TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in resolved_temporal_layers],
                "official_layers": resolved_official_layers,
                "bbox": bbox,
                "zoom": zoom,
            },
        )
        await session.commit()
        payload = serialize_preload_run(row) or {}

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
    return payload


async def start_startup_preload(
    *,
    bbox: str | None,
    zoom: int | None,
    width: int,
    height: int,
    temporal_layers: list[str] | None,
    official_layers: list[str] | None,
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


async def get_preload_status(run_key: str) -> dict[str, Any]:
    async with AsyncSessionLocal() as session:
        row = await get_preload_run(session, run_key)
        payload = serialize_preload_run(row) or {"run_key": run_key, "status": "missing"}
    payload["task_state"] = "running" if run_key in PRELOAD_TASKS else payload.get("status", "missing")
    details = payload.get("details") or {}
    payload["critical_ready"] = bool(details.get("critical_ready"))
    return payload
