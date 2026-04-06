from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import unicodedata
from typing import Any

from shapely.geometry import mapping, shape
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.farm import FarmEstablishment, FarmField, FarmPaddock
from app.models.humedad import AOIUnit
from app.models.materialized import SatelliteLayerSnapshot, UnitIndexSnapshot
from app.services.analysis import (
    get_alert_history,
    get_scope_snapshot,
    get_scope_weather_forecast,
    get_timeline_context,
)
from app.services.public_api import TEMPORAL_LAYER_CONFIGS


TEMPORAL_LAYER_ORDER = (
    "alerta_fusion",
    "rgb",
    "ndvi",
    "ndmi",
    "ndwi",
    "savi",
    "sar",
    "lst",
)
TREND_SERIES_ORDER = ("ndvi", "ndmi", "ndwi", "savi", "sar_vv_db")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _match_score(candidate: str, query: str) -> float:
    candidate_norm = _normalize_text(candidate)
    query_norm = _normalize_text(query)
    if not candidate_norm or not query_norm:
        return 0.0
    if candidate_norm == query_norm:
        return 1.0
    if candidate_norm.startswith(query_norm):
        return 0.96
    if query_norm in candidate_norm:
        return 0.88
    query_tokens = [token for token in query_norm.split() if token]
    if query_tokens and all(token in candidate_norm for token in query_tokens):
        return 0.78
    return 0.0


def _safe_float(value: Any, digits: int | None = None) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    if digits is None:
        return numeric
    return round(numeric, digits)


def _geometry_bbox(geojson: dict[str, Any] | None) -> list[float] | None:
    if not geojson:
        return None
    try:
        bounds = shape(geojson).bounds
    except Exception:
        return None
    return [round(float(value), 6) for value in bounds]


def _simplify_geometry(geojson: dict[str, Any] | None) -> dict[str, Any] | None:
    if not geojson:
        return None
    try:
        geom = shape(geojson)
        west, south, east, north = geom.bounds
        tolerance = max(abs(east - west), abs(north - south)) / 250.0
        tolerance = max(tolerance, 0.00005)
        simplified = geom.simplify(tolerance, preserve_topology=True)
        return mapping(simplified)
    except Exception:
        return geojson


def _field_metadata_payload(field: FarmField, establishment: FarmEstablishment | None, paddock_count: int | None = None) -> dict[str, Any]:
    return {
        "id": field.id,
        "name": field.name,
        "establishment_id": field.establishment_id,
        "establishment_name": establishment.name if establishment else None,
        "department": field.department,
        "padron_value": field.padron_value,
        "centroid_lat": _safe_float(field.centroid_lat, 6),
        "centroid_lon": _safe_float(field.centroid_lon, 6),
        "area_ha": _safe_float(field.area_ha, 2),
        "aoi_unit_id": field.aoi_unit_id,
        "bbox": _geometry_bbox(field.field_geometry_geojson),
        "geometry_geojson_simplified": _simplify_geometry(field.field_geometry_geojson),
        "paddock_count": paddock_count,
        "created_at": field.created_at.isoformat() if field.created_at else None,
        "updated_at": field.updated_at.isoformat() if field.updated_at else None,
    }


def _paddock_metadata_payload(
    paddock: FarmPaddock,
    field: FarmField | None,
    establishment: FarmEstablishment | None,
) -> dict[str, Any]:
    centroid_lat = None
    centroid_lon = None
    if paddock.geometry_geojson:
        try:
            centroid = shape(paddock.geometry_geojson).centroid
            centroid_lat = _safe_float(centroid.y, 6)
            centroid_lon = _safe_float(centroid.x, 6)
        except Exception:
            centroid_lat = None
            centroid_lon = None
    return {
        "id": paddock.id,
        "name": paddock.name,
        "field_id": paddock.field_id,
        "field_name": field.name if field else None,
        "establishment_id": field.establishment_id if field else None,
        "establishment_name": establishment.name if establishment else None,
        "department": field.department if field else None,
        "centroid_lat": centroid_lat,
        "centroid_lon": centroid_lon,
        "area_ha": _safe_float(paddock.area_ha, 2),
        "aoi_unit_id": paddock.aoi_unit_id,
        "bbox": _geometry_bbox(paddock.geometry_geojson),
        "geometry_geojson_simplified": _simplify_geometry(paddock.geometry_geojson),
        "display_order": paddock.display_order,
        "created_at": paddock.created_at.isoformat() if paddock.created_at else None,
        "updated_at": paddock.updated_at.isoformat() if paddock.updated_at else None,
    }


async def _get_field_row(session: AsyncSession, field_id: str) -> tuple[FarmField, FarmEstablishment | None]:
    result = await session.execute(
        select(FarmField, FarmEstablishment)
        .join(FarmEstablishment, FarmEstablishment.id == FarmField.establishment_id, isouter=True)
        .where(FarmField.id == field_id, FarmField.active.is_(True))
        .limit(1)
    )
    row = result.first()
    if row is None:
        raise ValueError("Campo no encontrado")
    return row[0], row[1]


async def _get_paddock_row(session: AsyncSession, paddock_id: str) -> tuple[FarmPaddock, FarmField, FarmEstablishment | None]:
    result = await session.execute(
        select(FarmPaddock, FarmField, FarmEstablishment)
        .join(FarmField, FarmField.id == FarmPaddock.field_id)
        .join(FarmEstablishment, FarmEstablishment.id == FarmField.establishment_id, isouter=True)
        .where(FarmPaddock.id == paddock_id, FarmPaddock.active.is_(True), FarmField.active.is_(True))
        .limit(1)
    )
    row = result.first()
    if row is None:
        raise ValueError("Potrero no encontrado")
    return row[0], row[1], row[2]


async def _resolve_active_unit(session: AsyncSession, *, aoi_unit_id: str | None) -> AOIUnit:
    if not aoi_unit_id:
        raise RuntimeError("La entidad no tiene aoi_unit_id asociado")
    unit = await session.get(AOIUnit, aoi_unit_id)
    if unit is None or not unit.active:
        raise ValueError("AOIUnit no encontrada")
    return unit


async def search_fields_for_mcp(session: AsyncSession, *, query: str, limit: int = 5) -> dict[str, Any]:
    rows = (
        await session.execute(
            select(FarmField, FarmEstablishment)
            .join(FarmEstablishment, FarmEstablishment.id == FarmField.establishment_id, isouter=True)
            .where(FarmField.active.is_(True))
            .order_by(FarmField.name)
        )
    ).all()
    ranked: list[dict[str, Any]] = []
    for field, establishment in rows:
        label = " ".join(part for part in [field.name, establishment.name if establishment else None, field.department] if part)
        score = _match_score(label, query)
        if score <= 0:
            continue
        ranked.append({**_field_metadata_payload(field, establishment), "match_score": round(score, 3)})
    ranked.sort(key=lambda item: (-float(item["match_score"]), (item.get("name") or "").lower()))
    return {"query": query, "total": len(ranked[:limit]), "items": ranked[:limit]}


async def search_paddocks_for_mcp(
    session: AsyncSession,
    *,
    query: str,
    field_id: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    stmt = (
        select(FarmPaddock, FarmField, FarmEstablishment)
        .join(FarmField, FarmField.id == FarmPaddock.field_id)
        .join(FarmEstablishment, FarmEstablishment.id == FarmField.establishment_id, isouter=True)
        .where(FarmPaddock.active.is_(True), FarmField.active.is_(True))
        .order_by(FarmField.name, FarmPaddock.display_order, FarmPaddock.name)
    )
    if field_id:
        stmt = stmt.where(FarmPaddock.field_id == field_id)
    rows = (await session.execute(stmt)).all()
    ranked: list[dict[str, Any]] = []
    for paddock, field, establishment in rows:
        label = " ".join(part for part in [paddock.name, field.name, establishment.name if establishment else None] if part)
        score = _match_score(label, query)
        if score <= 0:
            continue
        ranked.append({**_paddock_metadata_payload(paddock, field, establishment), "match_score": round(score, 3)})
    ranked.sort(key=lambda item: (-float(item["match_score"]), (item.get("field_name") or "").lower(), (item.get("name") or "").lower()))
    return {"query": query, "field_id": field_id, "total": len(ranked[:limit]), "items": ranked[:limit]}


async def get_field_metadata_for_mcp(session: AsyncSession, *, field_id: str) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    paddock_count = (
        await session.execute(select(FarmPaddock.id).where(FarmPaddock.field_id == field_id, FarmPaddock.active.is_(True)))
    ).all()
    return _field_metadata_payload(field, establishment, paddock_count=len(paddock_count))


async def get_paddock_metadata_for_mcp(session: AsyncSession, *, paddock_id: str) -> dict[str, Any]:
    paddock, field, establishment = await _get_paddock_row(session, paddock_id)
    return _paddock_metadata_payload(paddock, field, establishment)


async def get_field_current_status_for_mcp(session: AsyncSession, *, field_id: str) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=field.aoi_unit_id)
    payload = await get_scope_snapshot(session, scope="unidad", unit_id=unit.id, department=unit.department)
    return {
        "scope_type": "field",
        "scope_id": field.id,
        "aoi_unit_id": unit.id,
        "selection_label": field.name,
        "field": _field_metadata_payload(field, establishment),
        "status": payload,
    }


async def get_paddock_current_status_for_mcp(session: AsyncSession, *, paddock_id: str) -> dict[str, Any]:
    paddock, field, establishment = await _get_paddock_row(session, paddock_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=paddock.aoi_unit_id)
    payload = await get_scope_snapshot(session, scope="unidad", unit_id=unit.id, department=unit.department)
    return {
        "scope_type": "paddock",
        "scope_id": paddock.id,
        "aoi_unit_id": unit.id,
        "selection_label": paddock.name,
        "field": _field_metadata_payload(field, establishment),
        "paddock": _paddock_metadata_payload(paddock, field, establishment),
        "status": payload,
    }


async def get_field_timeline_context_for_mcp(
    session: AsyncSession,
    *,
    field_id: str,
    target_date: date,
    history_days: int = 30,
) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=field.aoi_unit_id)
    payload = await get_timeline_context(
        session,
        scope="unidad",
        unit_id=unit.id,
        department=unit.department,
        target_date=target_date,
        history_days=history_days,
    )
    return {
        "scope_type": "field",
        "scope_id": field.id,
        "aoi_unit_id": unit.id,
        "selection_label": field.name,
        "field": _field_metadata_payload(field, establishment),
        "timeline_context": payload,
    }


async def get_paddock_timeline_context_for_mcp(
    session: AsyncSession,
    *,
    paddock_id: str,
    target_date: date,
    history_days: int = 30,
) -> dict[str, Any]:
    paddock, field, establishment = await _get_paddock_row(session, paddock_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=paddock.aoi_unit_id)
    payload = await get_timeline_context(
        session,
        scope="unidad",
        unit_id=unit.id,
        department=unit.department,
        target_date=target_date,
        history_days=history_days,
    )
    return {
        "scope_type": "paddock",
        "scope_id": paddock.id,
        "aoi_unit_id": unit.id,
        "selection_label": paddock.name,
        "field": _field_metadata_payload(field, establishment),
        "paddock": _paddock_metadata_payload(paddock, field, establishment),
        "timeline_context": payload,
    }


async def get_field_alert_history_for_mcp(session: AsyncSession, *, field_id: str, days: int = 30) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=field.aoi_unit_id)
    history = await get_alert_history(session, scope="unidad", unit_id=unit.id, department=unit.department, limit=days)
    return {
        "scope_type": "field",
        "scope_id": field.id,
        "aoi_unit_id": unit.id,
        "days": days,
        "selection_label": field.name,
        "field": _field_metadata_payload(field, establishment),
        "alert_history": history,
    }


async def get_paddock_alert_history_for_mcp(session: AsyncSession, *, paddock_id: str, days: int = 30) -> dict[str, Any]:
    paddock, field, establishment = await _get_paddock_row(session, paddock_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=paddock.aoi_unit_id)
    history = await get_alert_history(session, scope="unidad", unit_id=unit.id, department=unit.department, limit=days)
    return {
        "scope_type": "paddock",
        "scope_id": paddock.id,
        "aoi_unit_id": unit.id,
        "days": days,
        "selection_label": paddock.name,
        "field": _field_metadata_payload(field, establishment),
        "paddock": _paddock_metadata_payload(paddock, field, establishment),
        "alert_history": history,
    }


def _extract_layer_numeric_value(layer_key: str, row: SatelliteLayerSnapshot) -> float | None:
    summary = row.summary_stats or {}
    metadata = row.metadata_extra or {}
    if layer_key == "ndvi":
        candidates = (summary.get("ndvi_mean"), summary.get("index_mean"), summary.get("mean"))
    elif layer_key == "ndwi":
        candidates = (summary.get("ndwi_mean"), summary.get("index_mean"), summary.get("mean"))
    elif layer_key == "savi":
        candidates = (summary.get("savi_mean"), summary.get("index_mean"), summary.get("mean"))
    elif layer_key == "ndmi":
        candidates = (summary.get("ndmi_mean"), summary.get("estimated_ndmi"), summary.get("mean"))
    elif layer_key == "sar":
        candidates = (summary.get("vv_db_mean"), summary.get("mean"), metadata.get("vv_db_mean"))
    else:
        candidates = ()
    for candidate in candidates:
        numeric = _safe_float(candidate, 4)
        if numeric is not None:
            return numeric
    return None


async def _build_historical_trend_payload(
    session: AsyncSession,
    *,
    scope_type: str,
    scope_id: str,
    selection_label: str,
    aoi_unit_id: str,
    department: str,
    days: int,
    field_metadata: dict[str, Any] | None = None,
    paddock_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    days = max(1, min(int(days or 30), 180))
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    start_at = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_at = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)

    index_rows = list(
        (
            await session.execute(
                select(UnitIndexSnapshot)
                .where(
                    UnitIndexSnapshot.unit_id == aoi_unit_id,
                    UnitIndexSnapshot.observed_at >= start_at,
                    UnitIndexSnapshot.observed_at < end_at,
                )
                .order_by(UnitIndexSnapshot.observed_at)
            )
        ).scalars().all()
    )
    layer_rows = list(
        (
            await session.execute(
                select(SatelliteLayerSnapshot)
                .where(
                    SatelliteLayerSnapshot.unit_id == aoi_unit_id,
                    SatelliteLayerSnapshot.observed_at >= start_at,
                    SatelliteLayerSnapshot.observed_at < end_at,
                    SatelliteLayerSnapshot.layer_key.in_(("ndvi", "ndmi", "ndwi", "savi", "sar")),
                )
                .order_by(SatelliteLayerSnapshot.layer_key, SatelliteLayerSnapshot.observed_at)
            )
        ).scalars().all()
    )

    series: dict[str, dict[str, Any]] = {
        "ndvi": {"available": False, "unit": "index", "points": [], "reason": "not_materialized"},
        "ndmi": {"available": False, "unit": "index", "points": [], "reason": "not_materialized"},
        "ndwi": {"available": False, "unit": "index", "points": [], "reason": "not_materialized"},
        "savi": {"available": False, "unit": "index", "points": [], "reason": "not_materialized"},
        "sar_vv_db": {"available": False, "unit": "dB", "points": [], "reason": "not_materialized"},
    }

    for row in index_rows:
        observed_date = row.observed_at.date().isoformat()
        ndmi_value = _safe_float(row.s2_ndmi_mean if row.s2_ndmi_mean is not None else row.estimated_ndmi, 4)
        sar_value = _safe_float(row.s1_vv_db_mean, 4)
        if ndmi_value is not None:
            series["ndmi"]["available"] = True
            series["ndmi"]["reason"] = None
            series["ndmi"]["points"].append({"date": observed_date, "value": ndmi_value, "source": "unit_index_snapshots"})
        if sar_value is not None:
            series["sar_vv_db"]["available"] = True
            series["sar_vv_db"]["reason"] = None
            series["sar_vv_db"]["points"].append({"date": observed_date, "value": sar_value, "source": "unit_index_snapshots"})

    layer_series_alias = {"ndvi": "ndvi", "ndmi": "ndmi", "ndwi": "ndwi", "savi": "savi", "sar": "sar_vv_db"}
    for row in layer_rows:
        alias = layer_series_alias.get(row.layer_key)
        if alias is None:
            continue
        numeric_value = _extract_layer_numeric_value(row.layer_key, row)
        if numeric_value is None:
            continue
        observed_date = row.observed_at.date().isoformat()
        if observed_date in {point["date"] for point in series[alias]["points"]}:
            continue
        series[alias]["available"] = True
        series[alias]["reason"] = None
        series[alias]["points"].append({"date": observed_date, "value": numeric_value, "source": "satellite_layer_snapshots"})

    latest_candidates = [point["date"] for payload in series.values() for point in payload["points"] if point.get("date")]
    latest_observed_date = max(latest_candidates) if latest_candidates else None
    missing_series = [name for name in TREND_SERIES_ORDER if not series[name]["available"]]
    for payload in series.values():
        payload["points"].sort(key=lambda item: item["date"])

    return {
        "scope_type": scope_type,
        "scope_id": scope_id,
        "selection_label": selection_label,
        "aoi_unit_id": aoi_unit_id,
        "department": department,
        "days": days,
        "latest_observed_date": latest_observed_date,
        "field": field_metadata,
        "paddock": paddock_metadata,
        "series": series,
        "missing_series": missing_series,
    }


async def get_field_historical_trend_for_mcp(session: AsyncSession, *, field_id: str, days: int = 30) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=field.aoi_unit_id)
    return await _build_historical_trend_payload(
        session,
        scope_type="field",
        scope_id=field.id,
        selection_label=field.name,
        aoi_unit_id=unit.id,
        department=unit.department,
        days=days,
        field_metadata=_field_metadata_payload(field, establishment),
    )


async def get_paddock_historical_trend_for_mcp(session: AsyncSession, *, paddock_id: str, days: int = 30) -> dict[str, Any]:
    paddock, field, establishment = await _get_paddock_row(session, paddock_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=paddock.aoi_unit_id)
    return await _build_historical_trend_payload(
        session,
        scope_type="paddock",
        scope_id=paddock.id,
        selection_label=paddock.name,
        aoi_unit_id=unit.id,
        department=unit.department,
        days=days,
        field_metadata=_field_metadata_payload(field, establishment),
        paddock_metadata=_paddock_metadata_payload(paddock, field, establishment),
    )


def _latest_layer_coverage_payload(row: SatelliteLayerSnapshot) -> dict[str, Any]:
    metadata = row.metadata_extra or {}
    config = TEMPORAL_LAYER_CONFIGS.get(row.layer_key, {})
    public_id = config.get("public_id", row.layer_key)
    availability = str(metadata.get("availability") or ("available" if row.availability_score and row.availability_score > 0 else "missing"))
    visual_empty = bool(metadata.get("visual_empty", False))
    if availability in {"missing", "empty"} or visual_empty:
        visual_state = "empty"
    elif bool(metadata.get("is_interpolated", False)):
        visual_state = "interpolated"
    else:
        visual_state = "ready"
    return {
        "layer_id": public_id,
        "layer_key": row.layer_key,
        "display_name": config.get("label", row.layer_key),
        "observed_at": row.observed_at.isoformat() if row.observed_at else None,
        "source_mode": row.source_mode,
        "availability_score": _safe_float(row.availability_score, 2),
        "primary_source_date": metadata.get("primary_source_date"),
        "secondary_source_date": metadata.get("secondary_source_date"),
        "cloud_pixel_pct": _safe_float(metadata.get("cloud_pixel_pct"), 2),
        "renderable_pixel_pct": _safe_float(metadata.get("renderable_pixel_pct"), 2),
        "valid_pixel_pct": _safe_float(metadata.get("valid_pixel_pct"), 2),
        "visual_empty": visual_empty,
        "visual_state": visual_state,
        "availability": availability,
        "is_interpolated": bool(metadata.get("is_interpolated", False)),
    }


async def get_latest_satellite_coverage_for_mcp(session: AsyncSession, *, field_id: str) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=field.aoi_unit_id)
    rows = list(
        (
            await session.execute(
                select(SatelliteLayerSnapshot)
                .where(
                    SatelliteLayerSnapshot.unit_id == unit.id,
                    SatelliteLayerSnapshot.layer_key.in_(TEMPORAL_LAYER_ORDER),
                )
                .order_by(SatelliteLayerSnapshot.layer_key, desc(SatelliteLayerSnapshot.observed_at))
            )
        ).scalars().all()
    )
    latest_by_layer: dict[str, SatelliteLayerSnapshot] = {}
    for row in rows:
        latest_by_layer.setdefault(row.layer_key, row)
    layers = [_latest_layer_coverage_payload(latest_by_layer[layer_key]) for layer_key in TEMPORAL_LAYER_ORDER if layer_key in latest_by_layer]
    latest_observed_date = max((layer["observed_at"] for layer in layers if layer.get("observed_at")), default=None)
    return {
        "field_id": field.id,
        "aoi_unit_id": unit.id,
        "selection_label": field.name,
        "field": _field_metadata_payload(field, establishment),
        "latest_observed_date": latest_observed_date,
        "source_mode": "warehouse_snapshots",
        "layers": layers,
    }


def _alert_severity_from_risk(risk_score: float | None) -> str:
    score = float(risk_score or 0.0)
    if score >= 75:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def _alert_kind_from_state(payload: dict[str, Any]) -> str:
    raw_metrics = payload.get("raw_metrics") or {}
    spi_30d = _safe_float(raw_metrics.get("spi_30d"))
    if spi_30d is not None and spi_30d <= -0.7:
        return "sequia_hidrica"
    return "deterioro_hidrico"


async def get_active_weather_alerts_for_mcp(session: AsyncSession, *, field_id: str) -> dict[str, Any]:
    field, establishment = await _get_field_row(session, field_id)
    unit = await _resolve_active_unit(session, aoi_unit_id=field.aoi_unit_id)
    status = await get_scope_snapshot(session, scope="unidad", unit_id=unit.id, department=unit.department)
    forecast = await get_scope_weather_forecast(session, scope="unidad", unit_id=unit.id, department=unit.department)
    alerts: list[dict[str, Any]] = []

    risk_score = _safe_float(status.get("risk_score"), 1)
    if bool(status.get("actionable")) or (risk_score is not None and risk_score >= 45):
        alerts.append(
            {
                "id": f"current-{field.id}",
                "type": _alert_kind_from_state(status),
                "severity": _alert_severity_from_risk(risk_score),
                "active": True,
                "source": "current_status",
                "state": status.get("state"),
                "risk_score": risk_score,
                "summary": status.get("explanation") or "Riesgo hidrico activo en el campo.",
                "observed_at": status.get("observed_at"),
            }
        )

    for day in forecast.get("forecast", []):
        expected_risk = _safe_float(day.get("expected_risk"), 1)
        if expected_risk is None or expected_risk < 55:
            continue
        alerts.append(
            {
                "id": f"forecast-{field.id}-{day.get('date')}",
                "type": "riesgo_hidrico_proyectado",
                "severity": _alert_severity_from_risk(expected_risk),
                "active": True,
                "source": "forecast",
                "forecast_date": day.get("date"),
                "risk_score": expected_risk,
                "summary": day.get("escalation_reason") or "Presion hidrica proyectada por pronostico.",
                "precip_mm": _safe_float(day.get("precip_mm"), 1),
                "et0_mm": _safe_float(day.get("et0_mm"), 1),
            }
        )

    return {
        "field_id": field.id,
        "aoi_unit_id": unit.id,
        "selection_label": field.name,
        "generated_at": _now_utc().isoformat(),
        "field": _field_metadata_payload(field, establishment),
        "current_status": {
            "state": status.get("state"),
            "risk_score": risk_score,
            "confidence_score": _safe_float(status.get("confidence_score"), 1),
            "observed_at": status.get("observed_at"),
        },
        "alerts": alerts,
        "total_active": len(alerts),
    }
