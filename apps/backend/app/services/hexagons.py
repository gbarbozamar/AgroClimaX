from __future__ import annotations

from datetime import date, datetime, timezone
import random
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import AlertState
from app.models.humedad import AOIUnit, SatelliteObservation
from app.services.analysis import (
    _clamp,
    _compute_scores,
    _format_state_payload,
    _infer_soil_context,
    _resolve_calibration,
    _seed,
    _state_definitions_from_rules,
    _state_from_risk,
    _summarize_spatial_risk,
    ensure_latest_daily_analysis,
)
from app.services.business_settings import get_effective_alert_rules
from app.services.catalog import seed_catalog_units
from app.services.warehouse import build_feature_collection, get_cached_layer_features, materialize_unit_payload

try:
    import h3
except Exception:  # pragma: no cover
    h3 = None


def _is_state_current(state: AlertState | None, target_date: date) -> bool:
    if state is None or state.observed_at is None:
        return False
    observed_at = state.observed_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.astimezone(timezone.utc).date() == target_date


def _h3_resolution() -> int:
    return int(settings.hex_display_resolution or settings.default_hex_resolution)


def _h3_boundary_geometry(cell_id: str) -> dict[str, Any]:
    if h3 is None:
        raise RuntimeError("h3 no disponible")
    boundary = h3.cell_to_boundary(cell_id)
    ring = [[round(lon, 6), round(lat, 6)] for lat, lon in boundary]
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return {"type": "Polygon", "coordinates": [ring]}


def _geojson_to_h3_cells(geometry_geojson: dict[str, Any] | None, resolution: int) -> list[str]:
    if h3 is None or not geometry_geojson:
        return []
    try:
        if hasattr(h3, "geo_to_cells"):
            return sorted(h3.geo_to_cells(geometry_geojson, resolution))
        if hasattr(h3, "polygon_to_cells"):
            return sorted(h3.polygon_to_cells(geometry_geojson, resolution))
    except Exception:
        return []
    return []


async def seed_h3_units(session: AsyncSession, *, department: str | None = None) -> dict[str, Any]:
    await seed_catalog_units(session)
    resolution = _h3_resolution()
    query = select(AOIUnit).where(AOIUnit.unit_type == "department", AOIUnit.active.is_(True)).order_by(AOIUnit.department)
    if department:
        query = query.where(AOIUnit.department == department)
    result = await session.execute(query)
    department_units = list(result.scalars().all())

    existing_query = select(AOIUnit).where(AOIUnit.unit_type == "h3_cell")
    if department:
        existing_query = existing_query.where(AOIUnit.department == department)
    existing_result = await session.execute(existing_query)
    existing_units = {unit.id: unit for unit in existing_result.scalars().all()}

    created = 0
    updated = 0
    planned = 0
    for department_unit in department_units:
        cells = _geojson_to_h3_cells(department_unit.geometry_geojson, resolution)
        for cell_id in cells:
            planned += 1
            center_lat, center_lon = h3.cell_to_latlng(cell_id)
            geometry = _h3_boundary_geometry(cell_id)
            unit_id = f"h3-r{resolution}-{cell_id}"
            metadata_extra = {
                "h3_index": cell_id,
                "h3_resolution": resolution,
                "parent_department_unit_id": department_unit.id,
                "fallback_role": "product_unit_fallback",
            }
            existing = existing_units.get(unit_id)
            if existing is None:
                session.add(
                    AOIUnit(
                        id=unit_id,
                        slug=unit_id,
                        unit_type="h3_cell",
                        scope="unidad",
                        name=f"H3 {cell_id[-7:].upper()} - {department_unit.department}",
                        department=department_unit.department,
                        geometry_geojson=geometry,
                        centroid_lat=round(center_lat, 6),
                        centroid_lon=round(center_lon, 6),
                        coverage_class=department_unit.coverage_class,
                        source=f"h3_r{resolution}_department_fallback",
                        data_mode="derived_department",
                        metadata_extra=metadata_extra,
                    )
                )
                created += 1
                continue

            changed = False
            if existing.name != f"H3 {cell_id[-7:].upper()} - {department_unit.department}":
                existing.name = f"H3 {cell_id[-7:].upper()} - {department_unit.department}"
                changed = True
            if existing.geometry_geojson != geometry:
                existing.geometry_geojson = geometry
                changed = True
            if existing.centroid_lat != round(center_lat, 6) or existing.centroid_lon != round(center_lon, 6):
                existing.centroid_lat = round(center_lat, 6)
                existing.centroid_lon = round(center_lon, 6)
                changed = True
            if existing.coverage_class != department_unit.coverage_class:
                existing.coverage_class = department_unit.coverage_class
                changed = True
            if existing.source != f"h3_r{resolution}_department_fallback":
                existing.source = f"h3_r{resolution}_department_fallback"
                changed = True
            if existing.metadata_extra != metadata_extra:
                existing.metadata_extra = metadata_extra
                changed = True
            if changed:
                updated += 1

    if created or updated:
        await session.commit()
    return {
        "resolution": resolution,
        "department_filter": department,
        "planned_cells": planned,
        "created": created,
        "updated": updated,
    }


async def _latest_department_observations(session: AsyncSession, department_unit_ids: list[str]) -> dict[str, SatelliteObservation]:
    if not department_unit_ids:
        return {}
    result = await session.execute(
        select(SatelliteObservation)
        .where(SatelliteObservation.unit_id.in_(department_unit_ids))
        .order_by(desc(SatelliteObservation.observed_at))
    )
    observations: dict[str, SatelliteObservation] = {}
    for item in result.scalars().all():
        observations.setdefault(item.unit_id, item)
    return observations


def _derive_hex_observation(
    unit: AOIUnit,
    department_unit: AOIUnit,
    department_state: AlertState,
    department_observation: SatelliteObservation | None,
) -> dict[str, Any]:
    raw_metrics = department_state.raw_metrics or {}
    soil_context = _infer_soil_context(unit, unit.geometry_geojson)
    rng = random.Random(_seed(unit.id, date.today().isoformat(), "hex-derived"))
    lat_gradient = (unit.centroid_lat or 0.0) - (department_unit.centroid_lat or 0.0)
    lon_gradient = (unit.centroid_lon or 0.0) - (department_unit.centroid_lon or 0.0)
    vulnerability_push = (soil_context.get("vulnerability_score", 50.0) - 50.0) / 50.0
    local_modifier = (lat_gradient * 15.0) - (lon_gradient * 12.0) + rng.uniform(-1.2, 1.2)

    base_humidity = raw_metrics.get("s1_humidity_mean_pct")
    if base_humidity is None and department_observation is not None:
        base_humidity = department_observation.s1_humidity_mean_pct
    if base_humidity is None:
        base_humidity = 47.0
    humidity = round(_clamp(base_humidity - local_modifier * 4.6 - vulnerability_push * 8.5, 2.0, 98.0), 1)

    base_ndmi = raw_metrics.get("s2_ndmi_mean")
    if base_ndmi is None:
        base_ndmi = raw_metrics.get("estimated_ndmi")
    if base_ndmi is None and department_observation is not None:
        base_ndmi = department_observation.s2_ndmi_mean
    if base_ndmi is None:
        base_ndmi = 0.02
    ndmi = round(max(-0.45, min(0.6, base_ndmi - local_modifier * 0.031 - vulnerability_push * 0.05)), 3)

    base_vv = raw_metrics.get("s1_vv_db_mean")
    if base_vv is None and department_observation is not None:
        base_vv = department_observation.s1_vv_db_mean
    if base_vv is None:
        base_vv = -12.0
    vv_db = round(max(-22.0, min(-4.0, base_vv + ((humidity - base_humidity) * 0.08) + rng.uniform(-0.45, 0.45))), 3)

    base_spi = raw_metrics.get("spi_30d")
    if base_spi is None and department_observation is not None:
        base_spi = department_observation.spi_30d
    if base_spi is None:
        base_spi = -0.5
    spi = round(max(-3.0, min(3.0, base_spi - lat_gradient * 0.2 + rng.uniform(-0.15, 0.15))), 3)

    department_qc = department_observation.quality_control if department_observation else {}
    valid_pct = department_observation.s2_valid_pct if department_observation and department_observation.s2_valid_pct is not None else 78.0
    cloud_pct = department_observation.cloud_cover_pct if department_observation and department_observation.cloud_cover_pct is not None else round(max(0.0, 100.0 - valid_pct), 1)
    lag_hours = department_observation.lag_hours if department_observation and department_observation.lag_hours is not None else 12.0
    quality_score = department_observation.quality_score if department_observation and department_observation.quality_score is not None else round(_clamp(department_state.confidence_score + 2.5), 1)
    pct_stressed = round(_clamp(department_state.affected_pct + local_modifier * 4.4 + vulnerability_push * 9.0, 0.0, 100.0), 1)

    return {
        "department": unit.department,
        "coverage_class": unit.coverage_class or "pastura_cultivo",
        "vegetation_mask": "vegetacion_densa" if ndmi > 0.28 else "vegetacion_media",
        "source_mode": "derived_department",
        "s1_vv_db_mean": vv_db,
        "s1_humidity_mean_pct": humidity,
        "s1_pct_area_stressed": pct_stressed,
        "s2_ndmi_mean": ndmi,
        "s2_valid_pct": valid_pct,
        "cloud_cover_pct": cloud_pct,
        "lag_hours": lag_hours,
        "spi_30d": spi,
        "spi_categoria": "seco" if spi <= -1.0 else ("humedo" if spi >= 1.0 else "normal"),
        "quality_score": quality_score,
        "quality_control": {
            "provider": "derived_department_h3_overlay",
            "freshness_days": float(department_qc.get("freshness_days", 1.0)),
            "coverage_valid_pct": valid_pct,
            "cloud_cover_pct": cloud_pct,
            "lag_hours": lag_hours,
            "geometry_source": unit.source,
            "fallback_reason": "h3_overlay_derived_from_department",
            "source_department": unit.department,
        },
        "raw_payload": {
            "source_department_unit": department_unit.id,
            "source_department_state": department_state.current_state,
            "h3_index": (unit.metadata_extra or {}).get("h3_index"),
            "h3_resolution": (unit.metadata_extra or {}).get("h3_resolution"),
        },
    }


def _state_payload_from_scores(
    unit: AOIUnit,
    department_state: AlertState,
    score_payload: dict[str, Any],
    observation: dict[str, Any],
    soil_context: dict[str, Any],
    spatial_summary: dict[str, Any],
    calibration_ref: str,
    rule_set: dict[str, Any],
    rules_version: str,
) -> dict[str, Any]:
    state_name = _state_from_risk(score_payload["risk_score"], rule_set)
    definition = _state_definitions_from_rules(rule_set)[state_name]
    metadata_extra = unit.metadata_extra or {}
    return {
        "scope": "unidad",
        "unit_id": unit.id,
        "unit_name": unit.name,
        "department": unit.department,
        "unit_type": unit.unit_type,
        "observed_at": department_state.observed_at.isoformat() if department_state.observed_at else None,
        "state": state_name,
        "state_level": definition["level"],
        "legacy_level": definition["legacy"],
        "color": definition["color"],
        "risk_score": round(score_payload["risk_score"], 1),
        "confidence_score": round(score_payload["confidence_score"], 1),
        "affected_pct": round(spatial_summary["affected_pct"], 1),
        "largest_cluster_pct": round(spatial_summary["largest_cluster_pct"], 1),
        "days_in_state": max(1, int(department_state.days_in_state or 1)),
        "actionable": spatial_summary["actionable"],
        "drivers": score_payload["drivers"],
        "forecast": score_payload["forecast"],
        "soil_context": soil_context,
        "calibration_ref": calibration_ref,
        "data_mode": observation.get("source_mode", "derived_department"),
        "geometry_source": unit.source,
        "h3_index": metadata_extra.get("h3_index"),
        "h3_resolution": metadata_extra.get("h3_resolution"),
        "fallback_role": metadata_extra.get("fallback_role"),
        "explanation": f"Hexagono H3 derivado desde {unit.department} como fallback operativo de unidad productiva.",
        "rules_version": rules_version,
        "raw_metrics": {
            "s1_vv_db_mean": round(observation.get("s1_vv_db_mean"), 3) if observation.get("s1_vv_db_mean") is not None else None,
            "s1_humidity_mean_pct": round(observation.get("s1_humidity_mean_pct"), 1) if observation.get("s1_humidity_mean_pct") is not None else None,
            "s2_ndmi_mean": round(observation.get("s2_ndmi_mean"), 3) if observation.get("s2_ndmi_mean") is not None else None,
            "spi_30d": round(observation.get("spi_30d"), 3) if observation.get("spi_30d") is not None else None,
            "estimated_ndmi": round(score_payload["estimated_ndmi"], 3) if score_payload.get("estimated_ndmi") is not None else None,
            "component_scores": score_payload.get("component_scores", {}),
        },
    }


async def _build_hex_snapshot(
    unit: AOIUnit,
    department_unit: AOIUnit,
    department_state: AlertState,
    department_observation: SatelliteObservation | None,
    calibration_cache: dict[tuple[str, str, str], Any],
    calibration_loader,
    rule_set: dict[str, Any],
    rules_version: str,
) -> dict[str, Any]:
    observation = _derive_hex_observation(unit, department_unit, department_state, department_observation)
    key = (unit.department, observation["coverage_class"], observation["vegetation_mask"])
    calibration = calibration_cache.get(key)
    if calibration is None:
        calibration = await calibration_loader(
            department=unit.department,
            coverage_class=observation["coverage_class"],
            vegetation_mask=observation["vegetation_mask"],
            observed_at=department_state.observed_at or datetime.now(timezone.utc),
            rule_set=rule_set,
        )
        calibration_cache[key] = calibration

    soil_context = _infer_soil_context(unit, unit.geometry_geojson)
    score_payload = _compute_scores(
        observation=observation,
        calibration=calibration,
        soil_context=soil_context,
        forecast_days=department_state.forecast or [],
        history_events=[],
        history_obs=[],
        ground_truth=[],
        rule_set=rule_set,
    )
    spatial_summary = _summarize_spatial_risk(
        score_payload["risk_score"],
        max(1, department_state.days_in_state or 1),
        unit.id,
        unit.geometry_geojson,
        rule_set,
    )
    calibration_ref = calibration.id if hasattr(calibration, "id") else calibration.get("id", "fixed-diaz-rivera-2026")
    return _state_payload_from_scores(unit, department_state, score_payload, observation, soil_context, spatial_summary, calibration_ref, rule_set, rules_version)


async def materialize_h3_cache(
    session: AsyncSession,
    *,
    target_date: date | None = None,
    department: str | None = None,
    ensure_base_analysis: bool = True,
    persist_latest: bool = True,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    if ensure_base_analysis:
        await ensure_latest_daily_analysis(session, target_date=target_date)
    await seed_h3_units(session, department=department)

    query = select(AOIUnit).where(AOIUnit.unit_type == "h3_cell").order_by(AOIUnit.department, AOIUnit.name)
    if department:
        query = query.where(AOIUnit.department == department)
    hex_result = await session.execute(query)
    hex_units = list(hex_result.scalars().all())

    department_result = await session.execute(
        select(AOIUnit).where(AOIUnit.unit_type == "department").order_by(AOIUnit.department)
    )
    department_units = {unit.department: unit for unit in department_result.scalars().all()}

    state_result = await session.execute(select(AlertState).order_by(desc(AlertState.observed_at)))
    states = list(state_result.scalars().all())
    current_hex_states = {state.unit_id: state for state in states if state.scope == "unidad" and state.unit_id.startswith("h3-r")}
    department_states = {state.department: state for state in states if state.scope == "departamento"}
    department_observations = await _latest_department_observations(session, [unit.id for unit in department_units.values()])

    async def calibration_loader(**kwargs):
        return await _resolve_calibration(session, persist=False, **kwargs)

    calibration_cache: dict[tuple[str, str, str], Any] = {}
    payloads: list[dict[str, Any]] = []
    for hex_unit in hex_units:
        department_unit = department_units.get(hex_unit.department)
        department_state = department_states.get(hex_unit.department)
        if department_unit is None or department_state is None:
            continue
        resolved_rules = await get_effective_alert_rules(session, hex_unit.coverage_class)
        current_state = current_hex_states.get(hex_unit.id)
        if _is_state_current(current_state, target_date):
            payload = _format_state_payload(hex_unit, current_state, resolved_rules["rules"])
        else:
            payload = await _build_hex_snapshot(
                hex_unit,
                department_unit,
                department_state,
                department_observations.get(department_unit.id),
                calibration_cache,
                calibration_loader,
                resolved_rules["rules"],
                resolved_rules["rules_version"],
            )
        payload = {
            **payload,
            "geometry_source": hex_unit.source,
            "h3_index": (hex_unit.metadata_extra or {}).get("h3_index"),
            "h3_resolution": (hex_unit.metadata_extra or {}).get("h3_resolution"),
            "fallback_role": (hex_unit.metadata_extra or {}).get("fallback_role"),
            "summary_mode": "current_state" if _is_state_current(current_state, target_date) else "derived_department",
        }
        await materialize_unit_payload(
            session,
            hex_unit,
            payload,
            update_latest_cache=persist_latest,
            update_spatial_features=persist_latest,
        )
        payloads.append(payload)

    await session.flush()
    return {
        "count": len(payloads),
        "department_filter": department,
        "observed_at": str(target_date),
        "resolution": _h3_resolution(),
    }


async def list_h3_units(session: AsyncSession, department: str | None = None) -> list[dict[str, Any]]:
    rows = await get_cached_layer_features(session, layer_scope="hexagono", department=department)
    if not rows:
        await materialize_h3_cache(session, department=department)
        await session.commit()
        rows = await get_cached_layer_features(session, layer_scope="hexagono", department=department)
    return [row.properties or {} for row in rows]


async def h3_geojson(session: AsyncSession, department: str | None = None) -> dict[str, Any]:
    rows = await get_cached_layer_features(session, layer_scope="hexagono", department=department)
    if not rows:
        await materialize_h3_cache(session, department=department)
        await session.commit()
        rows = await get_cached_layer_features(session, layer_scope="hexagono", department=department)
    collection = build_feature_collection(rows, layer_scope="hexagonos_h3", department=department)
    collection["metadata"]["resolution"] = _h3_resolution()
    collection["metadata"]["source"] = "database_materialized_cache"
    return collection
