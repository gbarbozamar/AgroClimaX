from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
import random
from typing import Any

import httpx
from shapely.geometry import shape
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

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
from app.services.catalog import DEPARTMENTS, _normalize_department_name
from app.services.warehouse import (
    build_feature_collection,
    get_cached_layer_features,
    materialize_unit_payload,
)


BASE_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = BASE_DIR / ".catalog_cache"
SECTIONS_CACHE_FILE = CACHE_DIR / "uy_secciones_policiales.geojson"
SECTIONS_SOURCE_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/MapasBase/SNIG_Vectorial/MapServer/27/query"

DEPARTMENT_NAME_MAP = {
    _normalize_department_name(record.name): record.name
    for record in DEPARTMENTS
}
DEPARTMENT_NAME_MAP["tacuerembo"] = "Tacuarembo"


def _canonical_department_name(raw_value: str | None) -> str:
    if not raw_value:
        return "Sin departamento"
    normalized = _normalize_department_name(raw_value)
    return DEPARTMENT_NAME_MAP.get(normalized, str(raw_value).title())


async def _download_sections_geojson() -> dict[str, Any]:
    params = {
        "where": "1=1",
        "outFields": "DEPTO,SECCION,DEPARTAMENTO,Dptosec,superficie,OBJECTID,SP98",
        "returnGeometry": "true",
        "f": "geojson",
        "outSR": "4326",
        "geometryPrecision": "6",
    }
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        response = await client.get(SECTIONS_SOURCE_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SECTIONS_CACHE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _load_cached_sections() -> dict[str, Any] | None:
    if not SECTIONS_CACHE_FILE.exists():
        return None
    try:
        return json.loads(SECTIONS_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


async def load_police_sections_geojson(refresh: bool = False) -> tuple[dict[str, Any], str]:
    if refresh:
        try:
            return await _download_sections_geojson(), "snig_arcgis_live"
        except Exception:
            cached = _load_cached_sections()
            if cached is not None:
                return cached, "snig_arcgis_cache"
            raise

    cached = _load_cached_sections()
    if cached is not None:
        return cached, "snig_arcgis_cache"

    try:
        return await _download_sections_geojson(), "snig_arcgis_live"
    except Exception:
        cached = _load_cached_sections()
        if cached is not None:
            return cached, "snig_arcgis_cache"
        raise


def _section_unit_payloads(collection: dict[str, Any], geometry_source: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for feature in collection.get("features", []):
        properties = feature.get("properties", {})
        geometry = feature.get("geometry")
        section_code = str(properties.get("Dptosec") or "").strip()
        section_label = str(properties.get("SECCION") or "").strip() or f"SP {section_code[-2:]}"
        if not geometry or not section_code:
            continue
        centroid = shape(geometry).centroid
        department = _canonical_department_name(properties.get("DEPARTAMENTO"))
        payloads.append(
            {
                "id": f"section-police-{section_code}",
                "slug": f"seccion-policial-{section_code}",
                "name": f"Seccion Policial {section_label} - {department}",
                "department": department,
                "centroid_lat": round(centroid.y, 5),
                "centroid_lon": round(centroid.x, 5),
                "coverage_class": next(
                    (record.coverage_class for record in DEPARTMENTS if record.name == department),
                    "pastura_cultivo",
                ),
                "geometry_geojson": geometry,
                "geometry_source": geometry_source,
                "metadata_extra": {
                    "section_code": section_code,
                    "section_label": section_label,
                    "section_number": str(properties.get("SP98") or section_label.replace("SP", "").strip()),
                    "department_code": str(properties.get("DEPTO") or ""),
                    "source_objectid": properties.get("OBJECTID"),
                    "surface_m2": properties.get("superficie"),
                },
            }
        )
    payloads.sort(key=lambda item: (item["department"], item["metadata_extra"]["section_code"]))
    return payloads


async def seed_police_section_units(session: AsyncSession, refresh_geometries: bool = False) -> int:
    collection, geometry_source = await load_police_sections_geojson(refresh=refresh_geometries)
    payloads = _section_unit_payloads(collection, geometry_source)
    existing_result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "police_section"))
    existing_units = {unit.id: unit for unit in existing_result.scalars().all()}
    created = 0
    updated = 0

    for payload in payloads:
        existing = existing_units.get(payload["id"])
        if existing is None:
            session.add(
                AOIUnit(
                    id=payload["id"],
                    slug=payload["slug"],
                    unit_type="police_section",
                    scope="seccion",
                    name=payload["name"],
                    department=payload["department"],
                    geometry_geojson=payload["geometry_geojson"],
                    centroid_lat=payload["centroid_lat"],
                    centroid_lon=payload["centroid_lon"],
                    coverage_class=payload["coverage_class"],
                    source=payload["geometry_source"],
                    data_mode="derived_department",
                    metadata_extra=payload["metadata_extra"],
                )
            )
            created += 1
            continue

        merged_metadata = {**(existing.metadata_extra or {}), **payload["metadata_extra"]}
        changed = False
        if existing.name != payload["name"]:
            existing.name = payload["name"]
            changed = True
        if existing.department != payload["department"]:
            existing.department = payload["department"]
            changed = True
        if existing.geometry_geojson != payload["geometry_geojson"]:
            existing.geometry_geojson = payload["geometry_geojson"]
            changed = True
        if existing.centroid_lat != payload["centroid_lat"] or existing.centroid_lon != payload["centroid_lon"]:
            existing.centroid_lat = payload["centroid_lat"]
            existing.centroid_lon = payload["centroid_lon"]
            changed = True
        if existing.coverage_class != payload["coverage_class"]:
            existing.coverage_class = payload["coverage_class"]
            changed = True
        if existing.source != payload["geometry_source"]:
            existing.source = payload["geometry_source"]
            changed = True
        if existing.metadata_extra != merged_metadata:
            existing.metadata_extra = merged_metadata
            changed = True
        if changed:
            updated += 1

    if created or updated:
        await session.commit()
    return created + updated


def _is_state_current(state: AlertState | None, target_date: date) -> bool:
    if state is None or state.observed_at is None:
        return False
    observed_at = state.observed_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.astimezone(timezone.utc).date() == target_date


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
    return {
        "scope": unit.scope,
        "unit_id": unit.id,
        "unit_name": unit.name,
        "department": unit.department,
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
        "explanation": f"Desagregacion espacial desde {unit.department} para seccion policial.",
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


def _derive_section_observation(
    unit: AOIUnit,
    department_unit: AOIUnit,
    department_state: AlertState,
    department_observation: SatelliteObservation | None,
) -> dict[str, Any]:
    raw_metrics = department_state.raw_metrics or {}
    soil_context = _infer_soil_context(unit, unit.geometry_geojson)
    rng = random.Random(_seed(unit.id, date.today().isoformat(), "section-derived"))
    lat_gradient = (unit.centroid_lat or 0.0) - (department_unit.centroid_lat or 0.0)
    lon_gradient = (unit.centroid_lon or 0.0) - (department_unit.centroid_lon or 0.0)
    vulnerability_push = (soil_context.get("vulnerability_score", 50.0) - 50.0) / 50.0
    spatial_modifier = (lat_gradient * 12.0) - (lon_gradient * 8.0) + rng.uniform(-0.65, 0.65)

    base_humidity = raw_metrics.get("s1_humidity_mean_pct")
    if base_humidity is None and department_observation is not None:
        base_humidity = department_observation.s1_humidity_mean_pct
    if base_humidity is None:
        base_humidity = 48.0
    humidity = round(_clamp(base_humidity - spatial_modifier * 4.2 - vulnerability_push * 6.5, 4.0, 98.0), 1)

    base_ndmi = raw_metrics.get("s2_ndmi_mean")
    if base_ndmi is None:
        base_ndmi = raw_metrics.get("estimated_ndmi")
    if base_ndmi is None and department_observation is not None:
        base_ndmi = department_observation.s2_ndmi_mean
    if base_ndmi is None:
        base_ndmi = 0.02
    ndmi = round(max(-0.45, min(0.6, base_ndmi - spatial_modifier * 0.028 - vulnerability_push * 0.045)), 3)

    base_vv = raw_metrics.get("s1_vv_db_mean")
    if base_vv is None and department_observation is not None:
        base_vv = department_observation.s1_vv_db_mean
    if base_vv is None:
        base_vv = -12.2
    vv_db = round(max(-22.0, min(-4.0, base_vv + ((humidity - base_humidity) * 0.09) + rng.uniform(-0.35, 0.35))), 3)

    base_spi = raw_metrics.get("spi_30d")
    if base_spi is None and department_observation is not None:
        base_spi = department_observation.spi_30d
    if base_spi is None:
        base_spi = -0.6
    spi = round(max(-3.0, min(3.0, base_spi - lat_gradient * 0.18 + rng.uniform(-0.12, 0.12))), 3)

    department_qc = department_observation.quality_control if department_observation else {}
    valid_pct = department_observation.s2_valid_pct if department_observation and department_observation.s2_valid_pct is not None else 78.0
    cloud_pct = department_observation.cloud_cover_pct if department_observation and department_observation.cloud_cover_pct is not None else round(max(0.0, 100.0 - valid_pct), 1)
    lag_hours = department_observation.lag_hours if department_observation and department_observation.lag_hours is not None else 12.0
    quality_score = department_observation.quality_score if department_observation and department_observation.quality_score is not None else round(_clamp(department_state.confidence_score + 4.0), 1)
    pct_stressed = round(_clamp(department_state.affected_pct + spatial_modifier * 3.5 + vulnerability_push * 8.0, 0.0, 100.0), 1)

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
            "provider": "derived_department_disaggregation",
            "freshness_days": float(department_qc.get("freshness_days", 1.0)),
            "coverage_valid_pct": valid_pct,
            "cloud_cover_pct": cloud_pct,
            "lag_hours": lag_hours,
            "geometry_source": unit.source,
            "fallback_reason": "section_overlay_derived_from_department",
            "source_department": unit.department,
        },
        "raw_payload": {
            "source_department_unit": department_unit.id,
            "source_department_state": department_state.current_state,
            "section_code": (unit.metadata_extra or {}).get("section_code"),
        },
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


async def _build_section_snapshot(
    unit: AOIUnit,
    department_unit: AOIUnit,
    department_state: AlertState,
    department_observation: SatelliteObservation | None,
    calibration_cache: dict[tuple[str, str, str], Any],
    calibration_loader,
    rule_set: dict[str, Any],
    rules_version: str,
    previous_state: AlertState | None = None,
) -> dict[str, Any]:
    observation = _derive_section_observation(unit, department_unit, department_state, department_observation)
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
        previous_state.days_in_state if previous_state else max(1, department_state.days_in_state or 1),
        unit.id,
        unit.geometry_geojson,
        rule_set,
    )
    calibration_ref = calibration.id if hasattr(calibration, "id") else calibration.get("id", "fixed-diaz-rivera-2026")
    return _state_payload_from_scores(unit, department_state, score_payload, observation, soil_context, spatial_summary, calibration_ref, rule_set, rules_version)


async def materialize_police_section_cache(
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
    await seed_police_section_units(session)

    query = select(AOIUnit).where(AOIUnit.unit_type == "police_section").order_by(AOIUnit.department, AOIUnit.name)
    if department:
        query = query.where(AOIUnit.department == department)
    section_result = await session.execute(query)
    sections = list(section_result.scalars().all())

    department_result = await session.execute(
        select(AOIUnit).where(AOIUnit.unit_type == "department").order_by(AOIUnit.department)
    )
    department_units = {unit.department: unit for unit in department_result.scalars().all()}

    state_result = await session.execute(select(AlertState).order_by(desc(AlertState.observed_at)))
    states = list(state_result.scalars().all())
    section_states = {state.unit_id: state for state in states if state.scope == "seccion"}
    department_states = {state.department: state for state in states if state.scope == "departamento"}
    department_observations = await _latest_department_observations(
        session,
        [unit.id for unit in department_units.values()],
    )

    async def calibration_loader(**kwargs):
        return await _resolve_calibration(session, persist=False, **kwargs)

    calibration_cache: dict[tuple[str, str, str], Any] = {}
    payloads: list[dict[str, Any]] = []
    for section in sections:
        department_unit = department_units.get(section.department)
        department_state = department_states.get(section.department)
        if department_unit is None or department_state is None:
            continue
        resolved_rules = await get_effective_alert_rules(session, section.coverage_class)
        current_section_state = section_states.get(section.id)
        if _is_state_current(current_section_state, target_date):
            payload = _format_state_payload(section, current_section_state, resolved_rules["rules"])
        else:
            payload = await _build_section_snapshot(
                section,
                department_unit,
                department_state,
                department_observations.get(department_unit.id),
                calibration_cache,
                calibration_loader,
                resolved_rules["rules"],
                resolved_rules["rules_version"],
                current_section_state,
            )
        payload = {
            **payload,
            "section_code": (section.metadata_extra or {}).get("section_code"),
            "section_label": (section.metadata_extra or {}).get("section_label"),
            "section_number": (section.metadata_extra or {}).get("section_number"),
            "geometry_source": section.source,
            "summary_mode": "current_state" if _is_state_current(current_section_state, target_date) else "derived_department",
        }
        await materialize_unit_payload(
            session,
            section,
            payload,
            update_latest_cache=persist_latest,
            update_spatial_features=persist_latest,
        )
        payloads.append(payload)

    await session.flush()
    return {"count": len(payloads), "department_filter": department, "observed_at": str(target_date)}


async def list_police_sections(session: AsyncSession, department: str | None = None) -> list[dict[str, Any]]:
    rows = await get_cached_layer_features(session, layer_scope="seccion", department=department)
    if not rows:
        await materialize_police_section_cache(session, department=department)
        await session.commit()
        rows = await get_cached_layer_features(session, layer_scope="seccion", department=department)
    return [row.properties or {} for row in rows]


async def police_sections_geojson(session: AsyncSession, department: str | None = None) -> dict[str, Any]:
    rows = await get_cached_layer_features(session, layer_scope="seccion", department=department)
    if not rows:
        await materialize_police_section_cache(session, department=department)
        await session.commit()
        rows = await get_cached_layer_features(session, layer_scope="seccion", department=department)
    collection = build_feature_collection(rows, layer_scope="secciones_policiales", department=department)
    collection["metadata"]["source"] = "database_materialized_cache"
    return collection
