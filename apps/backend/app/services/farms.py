from __future__ import annotations

import hashlib
import logging
import math
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from statistics import mean
from typing import Any
from unittest.mock import AsyncMock

import httpx
from shapely.geometry import Point
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.ops import transform
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import AlertState
from app.models.auth import AppUser
from app.models.farm import FarmEstablishment, FarmField, FarmPaddock, PadronLookupCache
from app.models.humedad import AOIUnit
from app.services.analysis import (
    _format_state_payload,
    _payload_is_current,
    _state_definitions_from_rules,
    _state_from_risk,
    analyze_unit,
    ensure_latest_daily_analysis,
)
from app.services.catalog import _normalize_department_name, seed_catalog_units
from app.services.warehouse import get_cached_state_payload, materialize_unit_payload


logger = logging.getLogger(__name__)
PADRON_PROVIDER = "snig_padronario_rural"
PADRON_SERVICE_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/DGSA/LimitesAdministrativos/MapServer/0/query"
PADDOCK_CONTAINMENT_TOLERANCE_METERS = 10.0
# Tolerancia de solape entre potreros del mismo campo: solapes menores a esta
# franja (en metros) son aceptados (dibujos imperfectos a mano alzada, ruido
# del digitalizador). Se aplica erosionando el nuevo polígono por esta
# distancia y recién ahí chequeando overlap.
PADDOCK_OVERLAP_TOLERANCE_METERS = 15.0
ANALYTICS_NUMERIC_FIELDS = (
    "risk_score",
    "confidence_score",
    "affected_pct",
    "largest_cluster_pct",
    "days_in_state",
)
RAW_METRICS_NUMERIC_FIELDS = (
    "s1_humidity_mean_pct",
    "s1_vv_db_mean",
    "s2_ndmi_mean",
    "estimated_ndmi",
    "spi_30d",
)
FORECAST_NUMERIC_FIELDS = (
    "expected_risk",
    "precip_mm",
    "precip_probability_pct",
    "et0_mm",
    "temp_max_c",
    "temp_min_c",
    "humidity_mean_pct",
    "wind_mps",
    "wind_gust_mps",
    "wind_direction_deg",
    "spi_trend",
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _department_upper(value: str) -> str:
    return _normalize_department_name(value).upper()


def _slugify(value: str) -> str:
    return "-".join(part for part in _normalize_department_name(value).split() if part) or "campo"


def _geometry_shape(geojson: dict[str, Any] | None, *, field_name: str = "geometry") -> Polygon:
    if not geojson:
        raise ValueError(f"{field_name} es obligatorio")
    try:
        geometry = shape(geojson)
    except Exception as exc:
        raise ValueError(f"{field_name} invalido") from exc
    if geometry.is_empty:
        raise ValueError(f"{field_name} vacio")
    if not isinstance(geometry, Polygon):
        raise ValueError(f"{field_name} debe ser un Polygon continuo")
    return geometry


def _bbox(geometry: Polygon) -> list[float]:
    west, south, east, north = geometry.bounds
    return [round(float(west), 6), round(float(south), 6), round(float(east), 6), round(float(north), 6)]


def _approx_area_ha(geometry: Polygon) -> float:
    centroid = geometry.centroid
    lat_scale = 111_320.0
    lon_scale = 111_320.0 * math.cos(math.radians(centroid.y))
    projected = []
    for lon, lat in list(geometry.exterior.coords):
        projected.append((lon * lon_scale, lat * lat_scale))
    area_m2 = 0.0
    for index in range(len(projected) - 1):
        x1, y1 = projected[index]
        x2, y2 = projected[index + 1]
        area_m2 += (x1 * y2) - (x2 * y1)
    return round(abs(area_m2) / 2.0 / 10_000.0, 2)


def _project_geometry_local_meters(geometry: Polygon, *, reference_lat: float | None = None) -> Polygon:
    centroid = geometry.centroid
    anchor_lat = reference_lat if reference_lat is not None else centroid.y
    lat_scale = 111_320.0
    lon_scale = 111_320.0 * math.cos(math.radians(anchor_lat))
    if abs(lon_scale) < 1e-9:
        lon_scale = 1e-9
    return transform(lambda x, y, z=None: (x * lon_scale, y * lat_scale), geometry)


def _segment_midpoint(start: tuple[float, float], end: tuple[float, float]) -> tuple[float, float]:
    return ((start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0)


def _sample_polygon_points(geometry: Polygon | MultiPolygon) -> list[Point]:
    polygons = list(geometry.geoms) if isinstance(geometry, MultiPolygon) else [geometry]
    points: list[Point] = []
    for polygon in polygons:
        exterior = list(polygon.exterior.coords)
        for index, start in enumerate(exterior[:-1]):
            end = exterior[index + 1]
            points.append(Point(start))
            points.append(Point(_segment_midpoint(start, end)))
        for ring in polygon.interiors:
            interior = list(ring.coords)
            for index, start in enumerate(interior[:-1]):
                end = interior[index + 1]
                points.append(Point(start))
                points.append(Point(_segment_midpoint(start, end)))
        if polygon.representative_point():
            points.append(polygon.representative_point())
    return points


def _max_outside_distance_meters(field_geometry: Polygon, paddock_geometry: Polygon) -> float:
    reference_lat = field_geometry.centroid.y
    projected_field = _project_geometry_local_meters(field_geometry, reference_lat=reference_lat)
    projected_paddock = _project_geometry_local_meters(paddock_geometry, reference_lat=reference_lat)
    outside = projected_paddock.difference(projected_field)
    if outside.is_empty:
        return 0.0
    sample_points = _sample_polygon_points(outside)
    if not sample_points:
        return 0.0
    return max(point.distance(projected_field) for point in sample_points)


def _serialize_establishment(row: FarmEstablishment) -> dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "active": bool(row.active),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _serialize_field(row: FarmField, *, establishment: FarmEstablishment | None = None) -> dict[str, Any]:
    return {
        "id": row.id,
        "establishment_id": row.establishment_id,
        "establishment_name": establishment.name if establishment else None,
        "name": row.name,
        "department": row.department,
        "padron_value": row.padron_value,
        "padron_source": row.padron_source,
        "padron_lookup_payload": row.padron_lookup_payload or {},
        "padron_geometry_geojson": row.padron_geometry_geojson,
        "field_geometry_geojson": row.field_geometry_geojson,
        "centroid_lat": row.centroid_lat,
        "centroid_lon": row.centroid_lon,
        "area_ha": row.area_ha,
        "aoi_unit_id": row.aoi_unit_id,
        "active": bool(row.active),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _serialize_paddock(row: FarmPaddock) -> dict[str, Any]:
    return {
        "id": row.id,
        "field_id": row.field_id,
        "name": row.name,
        "geometry_geojson": row.geometry_geojson,
        "area_ha": row.area_ha,
        "aoi_unit_id": row.aoi_unit_id,
        "display_order": row.display_order,
        "active": bool(row.active),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _feature_collection(features: list[dict[str, Any]], *, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": metadata or {},
    }


def _round_or_none(value: Any, digits: int = 1) -> float | None:
    try:
        if value is None:
            return None
        return round(float(value), digits)
    except Exception:
        return None


def _weighted_average(values: list[tuple[float, float]], digits: int) -> float | None:
    filtered = [(float(value), float(weight)) for value, weight in values if value is not None and weight > 0]
    if not filtered:
        return None
    total_weight = sum(weight for _, weight in filtered)
    if total_weight <= 0:
        return None
    return round(sum(value * weight for value, weight in filtered) / total_weight, digits)


def _max_value(values: list[Any], digits: int = 1) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return round(max(numeric), digits)


def _forecast_peak_risk(forecast: list[dict[str, Any]] | None) -> float | None:
    if not forecast:
        return None
    risks = [float(item.get("expected_risk")) for item in forecast if item.get("expected_risk") is not None]
    if not risks:
        return None
    return round(max(risks), 1)


def _driver_name(driver: dict[str, Any]) -> str:
    return str(driver.get("name") or driver.get("key") or "sin_driver").strip()


def _summarize_payload(payload: dict[str, Any] | None, *, analytics_mode: str, area_ha: float | None, paddock_count: int | None = None) -> dict[str, Any] | None:
    if not payload:
        return None
    raw = payload.get("raw_metrics") or {}
    drivers = payload.get("drivers") or []
    primary_driver = drivers[0] if drivers else {}
    summary = {
        "analytics_mode": analytics_mode,
        "area_ha": _round_or_none(area_ha, 2),
        "paddock_count": paddock_count,
        "observed_at": payload.get("observed_at"),
        "state": payload.get("state"),
        "state_level": payload.get("state_level"),
        "risk_score": _round_or_none(payload.get("risk_score"), 1),
        "confidence_score": _round_or_none(payload.get("confidence_score"), 1),
        "affected_pct": _round_or_none(payload.get("affected_pct"), 1),
        "largest_cluster_pct": _round_or_none(payload.get("largest_cluster_pct"), 1),
        "days_in_state": payload.get("days_in_state"),
        "actionable": bool(payload.get("actionable")),
        "s1_humidity_mean_pct": _round_or_none(raw.get("s1_humidity_mean_pct"), 1),
        "s1_vv_db_mean": _round_or_none(raw.get("s1_vv_db_mean"), 3),
        "s2_ndmi_mean": _round_or_none(raw.get("s2_ndmi_mean"), 3),
        "estimated_ndmi": _round_or_none(raw.get("estimated_ndmi"), 3),
        "spi_30d": _round_or_none(raw.get("spi_30d"), 3),
        "forecast_peak_risk": _forecast_peak_risk(payload.get("forecast") or []),
        "primary_driver": _driver_name(primary_driver) if primary_driver else None,
        "primary_driver_score": _round_or_none(primary_driver.get("score"), 1) if primary_driver else None,
        "color": payload.get("color"),
        "data_mode": payload.get("data_mode"),
        "rules_version": payload.get("rules_version"),
    }
    return summary


def _coerce_weight(row_area: float | None) -> float:
    try:
        value = float(row_area or 0.0)
    except Exception:
        value = 0.0
    return value if value > 0 else 1.0


def _aggregate_forecast(entries: list[tuple[dict[str, Any], float]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[tuple[dict[str, Any], float]]] = defaultdict(list)
    for forecast_days, weight in entries:
        for item in forecast_days or []:
            forecast_date = str(item.get("date") or "").strip()
            if forecast_date:
                by_date[forecast_date].append((item, weight))
    aggregated: list[dict[str, Any]] = []
    for forecast_date in sorted(by_date)[:7]:
        rows = by_date[forecast_date]
        merged: dict[str, Any] = {"date": forecast_date, "source": "field_paddock_weighted"}
        for field_name in FORECAST_NUMERIC_FIELDS:
            digits = 3 if field_name in {"spi_trend"} else 1
            merged[field_name] = _weighted_average(
                [(item.get(field_name), weight) for item, weight in rows],
                digits,
            )
        if rows:
            representative = max(rows, key=lambda item: float(item[0].get("expected_risk") or 0.0))[0]
            merged["escalation_reason"] = representative.get("escalation_reason")
        aggregated.append(merged)
    return aggregated


def _aggregate_drivers(entries: list[tuple[list[dict[str, Any]], float]]) -> list[dict[str, Any]]:
    score_buckets: dict[str, list[tuple[float, float]]] = defaultdict(list)
    count_buckets: Counter[str] = Counter()
    detail_buckets: dict[str, Counter[str]] = defaultdict(Counter)
    for drivers, weight in entries:
        for driver in drivers or []:
            name = _driver_name(driver)
            score_buckets[name].append((driver.get("score"), weight))
            count_buckets[name] += 1
            detail = str(driver.get("detail") or "").strip()
            if detail:
                detail_buckets[name][detail] += 1
    ranked: list[dict[str, Any]] = []
    for name, bucket in score_buckets.items():
        ranked.append(
            {
                "name": name,
                "score": _weighted_average(bucket, 1) or 0.0,
                "detail": detail_buckets[name].most_common(1)[0][0] if detail_buckets[name] else f"presente en {count_buckets[name]} potreros",
            }
        )
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:5]


def _aggregate_component_scores(entries: list[tuple[dict[str, Any], float]]) -> dict[str, float]:
    buckets: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for raw_metrics, weight in entries:
        for name, value in (raw_metrics.get("component_scores") or {}).items():
            if value is not None:
                buckets[name].append((value, weight))
    return {
        name: score
        for name, score in (
            (name, _weighted_average(values, 1))
            for name, values in buckets.items()
        )
        if score is not None
    }


def _aggregate_field_payload(
    field_row: FarmField,
    establishment: FarmEstablishment,
    base_payload: dict[str, Any],
    paddock_rows: list[FarmPaddock],
    paddock_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    definitions = _state_definitions_from_rules()
    weighted_rows: list[tuple[FarmPaddock, dict[str, Any], float]] = []
    for paddock in paddock_rows:
        payload = paddock_payloads.get(paddock.id)
        if not payload:
            continue
        weighted_rows.append((paddock, payload, _coerce_weight(paddock.area_ha)))
    if not weighted_rows:
        return {
            **base_payload,
            "analytics_mode": "direct_field",
            "field_paddock_count": 0,
        }

    risk_score = _weighted_average([(payload.get("risk_score"), weight) for _, payload, weight in weighted_rows], 1) or 0.0
    confidence_score = _weighted_average([(payload.get("confidence_score"), weight) for _, payload, weight in weighted_rows], 1) or 0.0
    affected_pct = _weighted_average([(payload.get("affected_pct"), weight) for _, payload, weight in weighted_rows], 1) or 0.0
    largest_cluster_pct = _weighted_average([(payload.get("largest_cluster_pct"), weight) for _, payload, weight in weighted_rows], 1) or 0.0
    days_in_state = int(round(_weighted_average([(payload.get("days_in_state"), weight) for _, payload, weight in weighted_rows], 0) or 0))
    state_name = _state_from_risk(risk_score)
    state_definition = definitions[state_name]
    drivers = _aggregate_drivers([(payload.get("drivers") or [], weight) for _, payload, weight in weighted_rows])
    forecast = _aggregate_forecast([(payload.get("forecast") or [], weight) for _, payload, weight in weighted_rows])
    raw_metrics = {
        **(base_payload.get("raw_metrics") or {}),
        **{
            field_name: _weighted_average(
                [((payload.get("raw_metrics") or {}).get(field_name), weight) for _, payload, weight in weighted_rows],
                3 if field_name in {"s1_vv_db_mean", "s2_ndmi_mean", "estimated_ndmi", "spi_30d"} else 1,
            )
            for field_name in RAW_METRICS_NUMERIC_FIELDS
        },
        "component_scores": _aggregate_component_scores(
            [((payload.get("raw_metrics") or {}), weight) for _, payload, weight in weighted_rows]
        ),
        "aggregation_mode": "paddock_weighted",
        "aggregated_from_paddocks": [
            {
                "paddock_id": paddock.id,
                "aoi_unit_id": paddock.aoi_unit_id,
                "name": paddock.name,
                "area_ha": paddock.area_ha,
                "risk_score": payload.get("risk_score"),
            }
            for paddock, payload, _ in weighted_rows
        ],
    }
    observed_at = max(
        (payload.get("observed_at") for _, payload, _ in weighted_rows if payload.get("observed_at")),
        default=base_payload.get("observed_at"),
    )
    data_modes = Counter(str(payload.get("data_mode") or "simulated") for _, payload, _ in weighted_rows)
    rules_versions = {payload.get("rules_version") for _, payload, _ in weighted_rows if payload.get("rules_version")}
    actionable = any(bool(payload.get("actionable")) for _, payload, _ in weighted_rows)
    explanation = f"Campo agregado como promedio ponderado de {len(weighted_rows)} potreros activos."

    return {
        **base_payload,
        "scope": "unidad",
        "unit_id": base_payload.get("unit_id") or field_row.aoi_unit_id,
        "unit_name": field_row.name,
        "department": field_row.department,
        "observed_at": observed_at,
        "state": state_name,
        "state_level": state_definition["level"],
        "legacy_level": state_definition["legacy"],
        "color": state_definition["color"],
        "risk_score": risk_score,
        "confidence_score": confidence_score,
        "affected_pct": affected_pct,
        "largest_cluster_pct": largest_cluster_pct,
        "days_in_state": days_in_state,
        "actionable": actionable,
        "drivers": drivers,
        "forecast": forecast,
        "data_mode": next(iter(data_modes)) if len(data_modes) == 1 else "mixed",
        "explanation": explanation,
        "rules_version": next(iter(rules_versions)) if len(rules_versions) == 1 else (base_payload.get("rules_version") or "mixed"),
        "raw_metrics": raw_metrics,
        "soil_context": {
            **(base_payload.get("soil_context") or {}),
            "aggregation_mode": "paddock_weighted",
            "paddock_count": len(weighted_rows),
            "establishment_name": establishment.name,
        },
        "analytics_mode": "paddock_weighted",
        "field_paddock_count": len(weighted_rows),
    }


def _synthetic_testing_payload(unit: AOIUnit) -> dict[str, Any]:
    metadata = unit.metadata_extra or {}
    unit_category = str(metadata.get("unit_category") or "predio").strip().lower()
    if unit_category == "campo":
        state_name = "Alerta"
        state_level = 2
        risk_score = 63.0
        confidence_score = 75.0
        affected_pct = 28.0
        largest_cluster_pct = 14.0
        days_in_state = 5
        actionable = True
        drivers = [{"name": "base_campo", "score": 63.0, "detail": "Synthetic field"}]
        forecast = [{"date": str(date.today()), "expected_risk": 67.0, "temp_max_c": 29.0, "precip_mm": 1.2}]
        raw_metrics = {
            "s1_humidity_mean_pct": 29.0,
            "s1_vv_db_mean": -11.8,
            "s2_ndmi_mean": 0.04,
            "estimated_ndmi": 0.05,
            "spi_30d": -0.9,
            "component_scores": {"soil": 64.0, "weather": 62.0},
        }
    else:
        state_name = "Vigilancia"
        state_level = 1
        risk_score = 45.0
        confidence_score = 70.0
        affected_pct = 16.0
        largest_cluster_pct = 8.0
        days_in_state = 3
        actionable = False
        drivers = [{"name": "prueba_local", "score": 45.0, "detail": "Synthetic testing"}]
        forecast = [{"date": str(date.today()), "expected_risk": 49.0, "temp_max_c": 27.0, "precip_mm": 2.5}]
        raw_metrics = {
            "s1_humidity_mean_pct": 34.0,
            "s1_vv_db_mean": -10.9,
            "s2_ndmi_mean": 0.18,
            "estimated_ndmi": 0.2,
            "spi_30d": -0.5,
            "component_scores": {"soil": 46.0, "weather": 44.0},
        }
    state = AlertState(
        unit_id=unit.id,
        scope=unit.scope,
        department=unit.department,
        observed_at=datetime.now(timezone.utc),
        current_state=state_name,
        state_level=state_level,
        risk_score=risk_score,
        confidence_score=confidence_score,
        affected_pct=affected_pct,
        largest_cluster_pct=largest_cluster_pct,
        days_in_state=days_in_state,
        actionable=actionable,
        data_mode="synthetic_testing",
        drivers=drivers,
        forecast=forecast,
        soil_context={"soil_label": unit_category or "predio"},
        calibration_ref="testing-synthetic-v1",
        raw_metrics=raw_metrics,
        explanation="Synthetic testing payload",
        metadata_extra={"rules_version": "testing-synthetic-v1"},
    )
    return {
        **_format_state_payload(unit, state),
        "unit_category": metadata.get("unit_category", "predio"),
        "geometry_source": unit.source,
        "summary_mode": "productive_unit",
    }


async def _upsert_paddock_aoi_unit(
    session: AsyncSession,
    *,
    paddock_row: FarmPaddock,
    field_row: FarmField,
    establishment: FarmEstablishment,
) -> AOIUnit:
    geometry = _geometry_shape(paddock_row.geometry_geojson, field_name="geometry_geojson")
    centroid = geometry.centroid
    unit_id = paddock_row.aoi_unit_id or f"user-paddock-{paddock_row.id}"
    metadata_extra = {
        "source": "user_field",
        "unit_category": "potrero",
        "farm_paddock_id": paddock_row.id,
        "farm_field_id": field_row.id,
        "establishment_id": establishment.id,
        "establishment_name": establishment.name,
        "padron_value": field_row.padron_value,
        "private_owner_user_id": paddock_row.user_id,
    }
    row = await session.get(AOIUnit, unit_id)
    if row is None:
        row = AOIUnit(
            id=unit_id,
            slug=unit_id,
            unit_type="productive_unit",
            scope="unidad",
            name=paddock_row.name,
            department=field_row.department,
            geometry_geojson=paddock_row.geometry_geojson,
            centroid_lat=round(centroid.y, 6),
            centroid_lon=round(centroid.x, 6),
            coverage_class="pastura_cultivo",
            source="user_field",
            data_mode="derived_department",
            metadata_extra=metadata_extra,
            active=True,
        )
        session.add(row)
    else:
        row.slug = unit_id
        row.name = paddock_row.name
        row.department = field_row.department
        row.geometry_geojson = paddock_row.geometry_geojson
        row.centroid_lat = round(centroid.y, 6)
        row.centroid_lon = round(centroid.x, 6)
        row.source = "user_field"
        row.data_mode = "derived_department"
        row.metadata_extra = metadata_extra
        row.active = True
    await session.flush()
    paddock_row.aoi_unit_id = row.id
    return row


async def _ensure_current_unit_payload(session: AsyncSession, unit: AOIUnit, *, target_date: date) -> dict[str, Any]:
    cached = await get_cached_state_payload(session, scope=unit.scope, unit_id=unit.id, department=unit.department)
    if _payload_is_current(cached, target_date):
        return cached
    return await _refresh_unit_payload(session, unit=unit, target_date=target_date)


async def _refresh_unit_payload(session: AsyncSession, *, unit: AOIUnit, target_date: date) -> dict[str, Any]:
    if settings.app_env == "testing" and not isinstance(analyze_unit, AsyncMock):
        payload = _synthetic_testing_payload(unit)
    else:
        analysis = await analyze_unit(session, unit=unit, target_date=target_date, geojson=unit.geometry_geojson)
        payload = {
            **_format_state_payload(unit, analysis["state"]),
            "unit_category": (unit.metadata_extra or {}).get("unit_category", "predio"),
            "geometry_source": unit.source,
            "summary_mode": "productive_unit",
        }
    await materialize_unit_payload(
        session,
        unit,
        payload,
        update_latest_cache=True,
        update_spatial_features=True,
    )
    await session.commit()
    refreshed = await get_cached_state_payload(session, scope=unit.scope, unit_id=unit.id, department=unit.department)
    return refreshed or payload


async def _get_active_paddock_rows(session: AsyncSession, *, user_id: str, field_id: str) -> list[FarmPaddock]:
    result = await session.execute(
        select(FarmPaddock)
        .where(FarmPaddock.field_id == field_id, FarmPaddock.user_id == user_id, FarmPaddock.active.is_(True))
        .order_by(FarmPaddock.display_order, FarmPaddock.name)
    )
    return list(result.scalars().all())


async def _ensure_field_analytics_bundle(
    session: AsyncSession,
    *,
    field_row: FarmField,
    establishment: FarmEstablishment,
    paddock_rows: list[FarmPaddock] | None = None,
    target_date: date | None = None,
    force_refresh_field: bool = False,
    force_refresh_paddock_ids: set[str] | None = None,
    read_only: bool = False,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    if not read_only and not (settings.app_env == "testing" and not isinstance(analyze_unit, AsyncMock)):
        await ensure_latest_daily_analysis(session, target_date=target_date)
    field_unit = (
        await _upsert_field_aoi_unit(session, field_row=field_row, establishment=establishment)
        if not read_only
        else (await session.get(AOIUnit, field_row.aoi_unit_id) if field_row.aoi_unit_id else None)
    )
    paddocks = paddock_rows if paddock_rows is not None else await _get_active_paddock_rows(session, user_id=field_row.user_id, field_id=field_row.id)
    cached_field_payload = (
        await get_cached_state_payload(session, scope=field_unit.scope, unit_id=field_unit.id, department=field_unit.department)
        if field_unit is not None
        else None
    )
    paddock_payloads: dict[str, dict[str, Any]] = {}
    refresh_paddocks = force_refresh_paddock_ids or set()
    for paddock in paddocks:
        paddock_unit = (
            await _upsert_paddock_aoi_unit(session, paddock_row=paddock, field_row=field_row, establishment=establishment)
            if not read_only
            else (await session.get(AOIUnit, paddock.aoi_unit_id) if paddock.aoi_unit_id else None)
        )
        if paddock_unit is None:
            continue
        if read_only:
            paddock_payload = await get_cached_state_payload(
                session,
                scope=paddock_unit.scope,
                unit_id=paddock_unit.id,
                department=paddock_unit.department,
            )
            if paddock_payload:
                paddock_payloads[paddock.id] = paddock_payload
            continue
        if paddock.id in refresh_paddocks:
            paddock_payloads[paddock.id] = await _refresh_unit_payload(session, unit=paddock_unit, target_date=target_date)
        else:
            paddock_payloads[paddock.id] = await _ensure_current_unit_payload(session, paddock_unit, target_date=target_date)

    effective_field_payload = cached_field_payload or {}
    if paddock_payloads:
        if read_only:
            base_payload = cached_field_payload or next(iter(paddock_payloads.values()))
            effective_field_payload = _aggregate_field_payload(field_row, establishment, base_payload, paddocks, paddock_payloads)
            return {
                "field_payload": effective_field_payload,
                "paddock_payloads": paddock_payloads,
                "analytics_mode": effective_field_payload.get("analytics_mode") or "paddock_weighted",
            }
        direct_field_payload = (
            await _refresh_unit_payload(session, unit=field_unit, target_date=target_date)
            if force_refresh_field or not _payload_is_current(cached_field_payload, target_date)
            else (cached_field_payload or {})
        )
        effective_field_payload = _aggregate_field_payload(field_row, establishment, direct_field_payload, paddocks, paddock_payloads)
        await materialize_unit_payload(
            session,
            field_unit,
            effective_field_payload,
            update_latest_cache=True,
            update_spatial_features=True,
        )
        await session.commit()
        refreshed = await get_cached_state_payload(session, scope=field_unit.scope, unit_id=field_unit.id, department=field_unit.department)
        if refreshed:
            effective_field_payload = refreshed
    else:
        needs_direct_refresh = (
            force_refresh_field
            or not _payload_is_current(cached_field_payload, target_date)
            or str((cached_field_payload or {}).get("analytics_mode") or "").strip().lower() == "paddock_weighted"
        )
        if read_only:
            direct_field_payload = cached_field_payload or {}
        else:
            direct_field_payload = await (
                _refresh_unit_payload(session, unit=field_unit, target_date=target_date)
                if needs_direct_refresh
                else _ensure_current_unit_payload(session, field_unit, target_date=target_date)
            )
        effective_field_payload = {
            **direct_field_payload,
            "analytics_mode": "direct_field",
            "field_paddock_count": 0,
        }

    return {
        "field_payload": effective_field_payload,
        "paddock_payloads": paddock_payloads,
        "analytics_mode": effective_field_payload.get("analytics_mode") or ("paddock_weighted" if paddock_payloads else "direct_field"),
    }


async def materialize_field_analytics_for_unit(
    session: AsyncSession,
    *,
    field_unit: AOIUnit,
    target_date: date | None = None,
) -> dict[str, Any] | None:
    metadata = field_unit.metadata_extra or {}
    field_id = str(metadata.get("farm_field_id") or "").strip()
    if not field_id:
        return None
    field_row = await session.get(FarmField, field_id)
    if field_row is None or not field_row.active:
        return None
    establishment = await session.get(FarmEstablishment, field_row.establishment_id)
    if establishment is None or not establishment.active:
        return None
    return await _ensure_field_analytics_bundle(
        session,
        field_row=field_row,
        establishment=establishment,
        target_date=target_date,
    )


async def _department_options(session: AsyncSession) -> list[dict[str, str]]:
    async def _query_rows() -> list[Any]:
        result = await session.execute(
            select(AOIUnit.id, AOIUnit.department)
            .where(AOIUnit.unit_type == "department", AOIUnit.active.is_(True))
            .order_by(AOIUnit.department)
        )
        return result.all()

    rows = await _query_rows()
    if not rows:
        await seed_catalog_units(session)
        rows = await _query_rows()
    return [{"id": row.id, "label": row.department} for row in rows]


def _padron_query_key(department: str, padron_value: str) -> str:
    normalized = f"{_department_upper(department)}:{str(padron_value).strip()}"
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


async def search_padron(session: AsyncSession, *, department: str, padron_value: str) -> dict[str, Any]:
    clean_padron = str(padron_value or "").strip()
    if not clean_padron.isdigit():
        raise ValueError("El padron debe ser numerico")

    query_key = _padron_query_key(department, clean_padron)
    cached = await session.execute(select(PadronLookupCache).where(PadronLookupCache.query_key == query_key))
    cached_row = cached.scalar_one_or_none()
    if cached_row is not None:
        return {
            "found": bool(cached_row.geometry_geojson),
            "feature": (
                {
                    "type": "Feature",
                    "geometry": cached_row.geometry_geojson,
                    "properties": (cached_row.raw_payload or {}).get("properties") or {},
                }
                if cached_row.geometry_geojson
                else None
            ),
            "bbox": _bbox(shape(cached_row.geometry_geojson)) if cached_row.geometry_geojson else None,
            "area_ha": (cached_row.raw_payload or {}).get("properties", {}).get("AREAHA"),
            "raw_provider": cached_row.raw_payload or {},
            "provider": cached_row.provider,
            "cached": True,
        }

    normalized_department = _department_upper(department)
    params = {
        "where": f"PADRON = {int(clean_padron)} AND UPPER(DEPTO) = '{normalized_department}'",
        "outFields": "PADRON,DEPTO,AREAHA,DEPTOPADRON",
        "returnGeometry": "true",
        "f": "geojson",
        "outSR": "4326",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(PADRON_SERVICE_URL, params=params)
        response.raise_for_status()
        payload = response.json()

    features = payload.get("features") or []
    if len(features) != 1:
        cache_row = PadronLookupCache(
            department=normalized_department,
            padron_value=clean_padron,
            provider=PADRON_PROVIDER,
            query_key=query_key,
            geometry_geojson=None,
            raw_payload={"properties": {}, "raw_provider": payload, "result_count": len(features)},
            last_checked_at=_now_utc(),
        )
        session.add(cache_row)
        await session.commit()
        return {
            "found": False,
            "feature": None,
            "bbox": None,
            "area_ha": None,
            "raw_provider": payload,
            "provider": PADRON_PROVIDER,
            "cached": False,
        }

    feature = features[0]
    geometry_geojson = feature.get("geometry")
    geometry = _geometry_shape(geometry_geojson, field_name="padron_geometry_geojson")
    props = feature.get("properties") or {}
    cache_row = PadronLookupCache(
        department=normalized_department,
        padron_value=clean_padron,
        provider=PADRON_PROVIDER,
        query_key=query_key,
        geometry_geojson=geometry_geojson,
        centroid_lat=round(geometry.centroid.y, 6),
        centroid_lon=round(geometry.centroid.x, 6),
        raw_payload={"properties": props, "raw_provider": payload},
        last_checked_at=_now_utc(),
    )
    session.add(cache_row)
    await session.commit()
    return {
        "found": True,
        "feature": feature,
        "bbox": _bbox(geometry),
        "area_ha": props.get("AREAHA"),
        "raw_provider": payload,
        "provider": PADRON_PROVIDER,
        "cached": False,
    }


async def list_establishments(session: AsyncSession, *, user: AppUser) -> list[dict[str, Any]]:
    result = await session.execute(
        select(FarmEstablishment)
        .where(FarmEstablishment.user_id == user.id, FarmEstablishment.active.is_(True))
        .order_by(FarmEstablishment.name)
    )
    return [_serialize_establishment(item) for item in result.scalars().all()]


async def save_establishment(session: AsyncSession, *, user: AppUser, payload: dict[str, Any], establishment_id: str | None = None) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("El establecimiento debe tener nombre")

    row = await session.get(FarmEstablishment, establishment_id) if establishment_id else None
    if row is not None and row.user_id != user.id:
        raise ValueError("Establecimiento no encontrado")
    if row is None:
        row = FarmEstablishment(user_id=user.id)
        session.add(row)

    row.name = name
    row.description = str(payload.get("description") or "").strip() or None
    row.active = True
    await session.flush()
    await session.commit()
    return _serialize_establishment(row)


async def delete_establishment(session: AsyncSession, *, user: AppUser, establishment_id: str) -> dict[str, Any]:
    row = await session.get(FarmEstablishment, establishment_id)
    if row is None or row.user_id != user.id:
        raise ValueError("Establecimiento no encontrado")

    row.active = False
    fields_result = await session.execute(
        select(FarmField).where(FarmField.establishment_id == row.id, FarmField.user_id == user.id, FarmField.active.is_(True))
    )
    for field in fields_result.scalars().all():
        field.active = False
        paddocks_result = await session.execute(
            select(FarmPaddock).where(FarmPaddock.field_id == field.id, FarmPaddock.user_id == user.id, FarmPaddock.active.is_(True))
        )
        for paddock in paddocks_result.scalars().all():
            paddock.active = False
            if paddock.aoi_unit_id:
                paddock_aoi = await session.get(AOIUnit, paddock.aoi_unit_id)
                if paddock_aoi is not None:
                    paddock_aoi.active = False
        if field.aoi_unit_id:
            aoi = await session.get(AOIUnit, field.aoi_unit_id)
            if aoi is not None:
                aoi.active = False
    await session.commit()
    return {"status": "deleted", "id": establishment_id}


async def _get_active_establishment(session: AsyncSession, *, user_id: str, establishment_id: str) -> FarmEstablishment:
    row = await session.get(FarmEstablishment, establishment_id)
    if row is None or row.user_id != user_id or not row.active:
        raise ValueError("Establecimiento no encontrado")
    return row


async def _upsert_field_aoi_unit(session: AsyncSession, *, field_row: FarmField, establishment: FarmEstablishment) -> AOIUnit:
    geometry = _geometry_shape(field_row.field_geometry_geojson, field_name="field_geometry_geojson")
    centroid = geometry.centroid
    unit_id = field_row.aoi_unit_id or f"user-field-{field_row.id}"
    metadata_extra = {
        "source": "user_field",
        "farm_field_id": field_row.id,
        "establishment_id": establishment.id,
        "establishment_name": establishment.name,
        "padron_value": field_row.padron_value,
        "unit_category": "campo",
        "private_owner_user_id": field_row.user_id,
    }
    row = await session.get(AOIUnit, unit_id)
    if row is None:
        row = AOIUnit(
            id=unit_id,
            slug=unit_id,
            unit_type="productive_unit",
            scope="unidad",
            name=field_row.name,
            department=field_row.department,
            geometry_geojson=field_row.field_geometry_geojson,
            centroid_lat=round(centroid.y, 6),
            centroid_lon=round(centroid.x, 6),
            coverage_class="pastura_cultivo",
            source="user_field",
            data_mode="derived_department",
            metadata_extra=metadata_extra,
            active=True,
        )
        session.add(row)
    else:
        row.slug = unit_id
        row.name = field_row.name
        row.department = field_row.department
        row.geometry_geojson = field_row.field_geometry_geojson
        row.centroid_lat = round(centroid.y, 6)
        row.centroid_lon = round(centroid.x, 6)
        row.source = "user_field"
        row.data_mode = "derived_department"
        row.metadata_extra = metadata_extra
        row.active = True
    await session.flush()
    field_row.aoi_unit_id = row.id
    return row


async def list_fields(session: AsyncSession, *, user: AppUser, establishment_id: str | None = None) -> list[dict[str, Any]]:
    query = select(FarmField).where(FarmField.user_id == user.id, FarmField.active.is_(True))
    if establishment_id:
        query = query.where(FarmField.establishment_id == establishment_id)
    query = query.order_by(FarmField.name)
    field_rows = list((await session.execute(query)).scalars().all())
    if not field_rows:
        return []

    establishment_ids = sorted({item.establishment_id for item in field_rows})
    establishment_result = await session.execute(
        select(FarmEstablishment).where(
            FarmEstablishment.id.in_(establishment_ids),
            FarmEstablishment.user_id == user.id,
            FarmEstablishment.active.is_(True),
        )
    )
    establishments = {item.id: item for item in establishment_result.scalars().all()}
    serialized: list[dict[str, Any]] = []
    for item in field_rows:
        establishment = establishments.get(item.establishment_id)
        if establishment is None:
            continue
        analytics_bundle = await _ensure_field_analytics_bundle(
            session,
            field_row=item,
            establishment=establishment,
            read_only=True,
        )
        field_payload = analytics_bundle["field_payload"]
        paddock_payloads = analytics_bundle["paddock_payloads"]
        serialized.append(
            {
                **_serialize_field(item, establishment=establishment),
                "analytics_mode": analytics_bundle["analytics_mode"],
                "field_analytics": _summarize_payload(
                    field_payload,
                    analytics_mode=analytics_bundle["analytics_mode"],
                    area_ha=item.area_ha,
                    paddock_count=len(paddock_payloads),
                ),
            }
        )
    return sorted(serialized, key=lambda item: ((item.get("establishment_name") or "").lower(), (item.get("name") or "").lower()))


async def get_field(session: AsyncSession, *, user: AppUser, field_id: str) -> dict[str, Any]:
    field = await session.get(FarmField, field_id)
    if field is None or field.user_id != user.id or not field.active:
        raise ValueError("Campo no encontrado")
    establishment = await session.get(FarmEstablishment, field.establishment_id)
    if establishment is None or establishment.user_id != user.id or not establishment.active:
        raise ValueError("Campo no encontrado")
    paddock_rows = await _get_active_paddock_rows(session, user_id=user.id, field_id=field_id)
    analytics_bundle = await _ensure_field_analytics_bundle(
        session,
        field_row=field,
        establishment=establishment,
        paddock_rows=paddock_rows,
        read_only=True,
    )
    paddock_payloads = analytics_bundle["paddock_payloads"]
    paddocks = []
    for item in paddock_rows:
        paddocks.append(
            {
                **_serialize_paddock(item),
                "paddock_analytics": _summarize_payload(
                    paddock_payloads.get(item.id),
                    analytics_mode="paddock_direct",
                    area_ha=item.area_ha,
                ),
            }
        )
    return {
        **_serialize_field(field, establishment=establishment),
        "analytics_mode": analytics_bundle["analytics_mode"],
        "field_analytics": _summarize_payload(
            analytics_bundle["field_payload"],
            analytics_mode=analytics_bundle["analytics_mode"],
            area_ha=field.area_ha,
            paddock_count=len(paddocks),
        ),
        "paddocks": paddocks,
    }


async def save_field(session: AsyncSession, *, user: AppUser, payload: dict[str, Any], field_id: str | None = None) -> dict[str, Any]:
    establishment_id = str(payload.get("establishment_id") or "").strip()
    if not establishment_id:
        raise ValueError("Debes seleccionar un establecimiento")
    establishment = await _get_active_establishment(session, user_id=user.id, establishment_id=establishment_id)

    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("El campo debe tener nombre")
    department = str(payload.get("department") or "").strip()
    if not department:
        raise ValueError("El campo debe indicar departamento")
    padron_value = str(payload.get("padron_value") or "").strip()
    if not padron_value:
        raise ValueError("El campo debe indicar padron")

    field_geometry_geojson = payload.get("field_geometry_geojson")
    field_geometry = _geometry_shape(field_geometry_geojson, field_name="field_geometry_geojson")
    centroid = field_geometry.centroid
    field_area_ha = _approx_area_ha(field_geometry)

    row = await session.get(FarmField, field_id) if field_id else None
    if row is not None and (row.user_id != user.id or not row.active):
        raise ValueError("Campo no encontrado")
    _is_new_field = row is None
    if row is None:
        row = FarmField(user_id=user.id, establishment_id=establishment.id)
        session.add(row)

    row.establishment_id = establishment.id
    row.name = name
    row.department = department
    row.padron_value = padron_value
    row.padron_source = str(payload.get("padron_source") or PADRON_PROVIDER)
    row.padron_lookup_payload = payload.get("padron_lookup_payload") or {}
    row.padron_geometry_geojson = payload.get("padron_geometry_geojson")
    row.field_geometry_geojson = field_geometry_geojson
    row.centroid_lat = round(centroid.y, 6)
    row.centroid_lon = round(centroid.x, 6)
    row.area_ha = float(payload.get("area_ha") or field_area_ha or 0.0)
    row.active = True
    await session.flush()
    await _upsert_field_aoi_unit(session, field_row=row, establishment=establishment)
    await session.commit()
    field_id_value = row.id
    field_user_id_value = row.user_id
    try:
        await _ensure_field_analytics_bundle(
            session,
            field_row=row,
            establishment=establishment,
            force_refresh_field=True,
        )
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("No se pudo refrescar la analitica del campo %s durante el guardado", field_id_value)

    # Auto-backfill de snapshots para el campo nuevo. Corre en background task
    # para no bloquear el save. Si falla, log warning — el usuario puede
    # disparar manualmente desde la UI después.
    if _is_new_field:
        try:
            import asyncio
            from datetime import timedelta
            from app.db.session import AsyncSessionLocal

            async def _auto_backfill(fid: str, uid: str) -> None:
                from app.services.field_snapshots import render_field_snapshot
                today = date.today()
                for i in range(30):
                    target = today - timedelta(days=i)
                    for layer in ("ndvi", "ndmi", "alerta_fusion"):
                        async with AsyncSessionLocal() as bg_session:
                            try:
                                await render_field_snapshot(bg_session, fid, layer, target, user_id=uid)
                                await bg_session.commit()
                            except Exception:
                                await bg_session.rollback()
                logger.info("auto-backfill completed field=%s", fid)

            asyncio.create_task(_auto_backfill(field_id_value, field_user_id_value))
            logger.info("auto-backfill scheduled field=%s", field_id_value)
        except Exception as exc:
            logger.warning("auto-backfill failed to schedule field=%s exc=%s", field_id_value, exc)

    return await get_field(session, user=user, field_id=field_id_value)


async def delete_field(session: AsyncSession, *, user: AppUser, field_id: str) -> dict[str, Any]:
    row = await session.get(FarmField, field_id)
    if row is None or row.user_id != user.id or not row.active:
        raise ValueError("Campo no encontrado")
    row.active = False
    paddocks_result = await session.execute(
        select(FarmPaddock).where(FarmPaddock.field_id == row.id, FarmPaddock.user_id == user.id, FarmPaddock.active.is_(True))
    )
    for paddock in paddocks_result.scalars().all():
        paddock.active = False
        if paddock.aoi_unit_id:
            paddock_aoi = await session.get(AOIUnit, paddock.aoi_unit_id)
            if paddock_aoi is not None:
                paddock_aoi.active = False
    if row.aoi_unit_id:
        aoi = await session.get(AOIUnit, row.aoi_unit_id)
        if aoi is not None:
            aoi.active = False
    await session.commit()
    return {"status": "deleted", "id": field_id}


async def list_fields_geojson(session: AsyncSession, *, user: AppUser, establishment_id: str | None = None) -> dict[str, Any]:
    items = await list_fields(session, user=user, establishment_id=establishment_id)
    return _feature_collection(
        [
            {
                "type": "Feature",
                "geometry": item["field_geometry_geojson"],
                "properties": {
                    "field_id": item["id"],
                    "unit_id": item["aoi_unit_id"],
                    "unit_name": item["name"],
                    "department": item["department"],
                    "establishment_id": item["establishment_id"],
                    "establishment_name": item["establishment_name"],
                    "padron_value": item["padron_value"],
                    "area_ha": item["area_ha"],
                    "scope_type": "field",
                    "analytics_mode": item.get("analytics_mode"),
                    "analytics": item.get("field_analytics"),
                },
            }
            for item in items
            if item["field_geometry_geojson"]
        ],
        metadata={"count": len(items)},
    )


async def _get_active_field(session: AsyncSession, *, user_id: str, field_id: str) -> FarmField:
    row = await session.get(FarmField, field_id)
    if row is None or row.user_id != user_id or not row.active:
        raise ValueError("Campo no encontrado")
    return row


async def list_paddocks(session: AsyncSession, *, user: AppUser, field_id: str) -> list[dict[str, Any]]:
    field_row = await _get_active_field(session, user_id=user.id, field_id=field_id)
    establishment = await _get_active_establishment(session, user_id=user.id, establishment_id=field_row.establishment_id)
    paddock_rows = await _get_active_paddock_rows(session, user_id=user.id, field_id=field_id)
    analytics_bundle = await _ensure_field_analytics_bundle(
        session,
        field_row=field_row,
        establishment=establishment,
        paddock_rows=paddock_rows,
        read_only=True,
    )
    paddock_payloads = analytics_bundle["paddock_payloads"]
    return [
        {
            **_serialize_paddock(item),
            "paddock_analytics": _summarize_payload(
                paddock_payloads.get(item.id),
                analytics_mode="paddock_direct",
                area_ha=item.area_ha,
            ),
        }
        for item in paddock_rows
    ]


async def save_paddock(
    session: AsyncSession,
    *,
    user: AppUser,
    field_id: str,
    payload: dict[str, Any],
    paddock_id: str | None = None,
) -> dict[str, Any]:
    field_row = await _get_active_field(session, user_id=user.id, field_id=field_id)
    field_geometry = _geometry_shape(field_row.field_geometry_geojson, field_name="field_geometry_geojson")

    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("El potrero debe tener nombre")

    geometry_geojson = payload.get("geometry_geojson")
    geometry = _geometry_shape(geometry_geojson, field_name="geometry_geojson")
    reference_lat = field_geometry.centroid.y
    projected_field = _project_geometry_local_meters(field_geometry, reference_lat=reference_lat)
    projected_paddock = _project_geometry_local_meters(geometry, reference_lat=reference_lat)
    if not projected_field.buffer(PADDOCK_CONTAINMENT_TOLERANCE_METERS).covers(projected_paddock):
        outside_distance = _max_outside_distance_meters(field_geometry, geometry)
        exceeded_by = max(outside_distance - PADDOCK_CONTAINMENT_TOLERANCE_METERS, 0.0)
        raise ValueError(
            "El potrero queda {:.1f} m fuera del campo y supera la tolerancia operativa de 10 m por {:.1f} m".format(
                outside_distance,
                exceeded_by,
            )
        )

    row = await session.get(FarmPaddock, paddock_id) if paddock_id else None
    if row is not None and (row.user_id != user.id or row.field_id != field_id or not row.active):
        raise ValueError("Potrero no encontrado")

    existing_result = await session.execute(
        select(FarmPaddock)
        .where(FarmPaddock.field_id == field_id, FarmPaddock.user_id == user.id, FarmPaddock.active.is_(True))
        .order_by(FarmPaddock.display_order, FarmPaddock.name)
    )
    existing_paddocks = list(existing_result.scalars().all())
    for item in existing_paddocks:
        if row is not None and item.id == row.id:
            continue
        if item.name.strip().lower() == name.lower():
            raise ValueError("El nombre del potrero debe ser unico dentro del campo")
        existing_geometry = _geometry_shape(item.geometry_geojson, field_name="geometry_geojson")
        if existing_geometry.contains(geometry) or geometry.contains(existing_geometry):
            raise ValueError("Los potreros del mismo campo no pueden solaparse")
        # Overlap parcial: se tolera hasta PADDOCK_OVERLAP_TOLERANCE_METERS.
        # Erosionamos el nuevo polígono por esa distancia en metros y recién ahí
        # chequeamos si sigue solapando. Si el solape era menor a la tolerancia,
        # el buffer negativo lo come y el check pasa.
        if existing_geometry.overlaps(geometry):
            projected_existing = _project_geometry_local_meters(existing_geometry, reference_lat=reference_lat)
            projected_new = _project_geometry_local_meters(geometry, reference_lat=reference_lat)
            eroded_new = projected_new.buffer(-PADDOCK_OVERLAP_TOLERANCE_METERS)
            # Si el erosionado sigue solapando -> es overlap real, rechazamos.
            if not eroded_new.is_empty and projected_existing.overlaps(eroded_new):
                # Medir el solape real en metros para dar contexto al usuario
                overlap_area_m2 = projected_existing.intersection(projected_new).area
                raise ValueError(
                    "El potrero se solapa con '{}' en {:.0f} m² (tolerancia operativa {:.0f} m).".format(
                        item.name, overlap_area_m2, PADDOCK_OVERLAP_TOLERANCE_METERS,
                    )
                )

    if row is None:
        row = FarmPaddock(user_id=user.id, field_id=field_id)
        session.add(row)

    row.name = name
    row.geometry_geojson = geometry_geojson
    row.area_ha = _approx_area_ha(geometry)
    row.display_order = int(payload.get("display_order") or len(existing_paddocks) + 1)
    row.active = True
    await session.flush()
    establishment = await _get_active_establishment(session, user_id=user.id, establishment_id=field_row.establishment_id)
    await _upsert_paddock_aoi_unit(session, paddock_row=row, field_row=field_row, establishment=establishment)
    await session.commit()
    paddock_id_value = row.id
    try:
        await _ensure_field_analytics_bundle(
            session,
            field_row=field_row,
            establishment=establishment,
            force_refresh_paddock_ids={row.id},
        )
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("No se pudo refrescar la analitica del potrero %s durante el guardado", paddock_id_value)
    paddock_payload = None
    if row.aoi_unit_id:
        paddock_unit = await session.get(AOIUnit, row.aoi_unit_id)
        if paddock_unit is not None:
            paddock_payload = await get_cached_state_payload(session, scope=paddock_unit.scope, unit_id=paddock_unit.id, department=paddock_unit.department)
    return {
        **_serialize_paddock(row),
        "paddock_analytics": _summarize_payload(
            paddock_payload,
            analytics_mode="paddock_direct",
            area_ha=row.area_ha,
        ),
    }


async def delete_paddock(session: AsyncSession, *, user: AppUser, field_id: str, paddock_id: str) -> dict[str, Any]:
    field_row = await _get_active_field(session, user_id=user.id, field_id=field_id)
    row = await session.get(FarmPaddock, paddock_id)
    if row is None or row.user_id != user.id or row.field_id != field_id or not row.active:
        raise ValueError("Potrero no encontrado")
    row.active = False
    if row.aoi_unit_id:
        paddock_aoi = await session.get(AOIUnit, row.aoi_unit_id)
        if paddock_aoi is not None:
            paddock_aoi.active = False
    establishment = await _get_active_establishment(session, user_id=user.id, establishment_id=field_row.establishment_id)
    await _ensure_field_analytics_bundle(session, field_row=field_row, establishment=establishment)
    await session.commit()
    return {"status": "deleted", "id": paddock_id}


async def paddocks_geojson(session: AsyncSession, *, user: AppUser, field_id: str) -> dict[str, Any]:
    paddocks = await list_paddocks(session, user=user, field_id=field_id)
    return _feature_collection(
        [
            {
                "type": "Feature",
                "geometry": item["geometry_geojson"],
                "properties": {
                    "paddock_id": item["id"],
                    "field_id": item["field_id"],
                    "name": item["name"],
                    "area_ha": item["area_ha"],
                    "display_order": item["display_order"],
                    "aoi_unit_id": item.get("aoi_unit_id"),
                    "analytics": item.get("paddock_analytics"),
                },
            }
            for item in paddocks
            if item["geometry_geojson"]
        ],
        metadata={"count": len(paddocks)},
    )


async def get_farm_options(session: AsyncSession, *, user: AppUser) -> dict[str, Any]:
    return {
        "departments": await _department_options(session),
        "establishments": await list_establishments(session, user=user),
    }


async def get_field_for_subscription(session: AsyncSession, *, user_id: str, field_id: str) -> FarmField | None:
    row = await session.get(FarmField, field_id)
    if row is None or row.user_id != user_id or not row.active:
        return None
    return row


async def get_field_by_aoi_unit_id(session: AsyncSession, *, aoi_unit_id: str) -> FarmField | None:
    result = await session.execute(
        select(FarmField).where(FarmField.aoi_unit_id == aoi_unit_id, FarmField.active.is_(True))
    )
    return result.scalar_one_or_none()


async def list_field_overlay_features(session: AsyncSession, *, field_id: str) -> list[dict[str, Any]]:
    result = await session.execute(
        select(FarmField, FarmEstablishment)
        .join(FarmEstablishment, FarmEstablishment.id == FarmField.establishment_id)
        .where(FarmField.id == field_id, FarmField.active.is_(True), FarmEstablishment.active.is_(True))
        .limit(1)
    )
    field_pair = result.first()
    field_row = field_pair[0] if field_pair else None
    establishment = field_pair[1] if field_pair else None
    paddock_rows = await _get_active_paddock_rows(session, user_id=field_row.user_id, field_id=field_id) if field_row else []
    paddock_payloads = {}
    if field_row and establishment:
        paddock_payloads = (
            await _ensure_field_analytics_bundle(
                session,
                field_row=field_row,
                establishment=establishment,
                paddock_rows=paddock_rows,
                read_only=True,
            )
        )["paddock_payloads"]
    return [
        {
            "label": item.name,
            "kind": "paddock",
            "geometry_geojson": item.geometry_geojson,
            "analytics": _summarize_payload(
                paddock_payloads.get(item.id),
                analytics_mode="paddock_direct",
                area_ha=item.area_ha,
            ),
        }
        for item in paddock_rows
        if item.geometry_geojson
    ]
