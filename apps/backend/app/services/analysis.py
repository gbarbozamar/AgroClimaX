from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import math
import random
from statistics import mean
from typing import Any

import httpx
import numpy as np
from shapely.geometry import shape
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import AlertState, AlertaEvento
from app.models.humedad import (
    AOIUnit,
    CalibrationSnapshot,
    ForecastSignal,
    GroundTruthMeasurement,
    HumedadSuelo,
    IngestionQualityLog,
    SatelliteObservation,
)
from app.models.materialized import UnitIndexSnapshot
from app.services.catalog import DEPARTMENTS, seed_catalog_units
from app.services.business_settings import DEFAULT_ALERT_RULESET, get_effective_alert_rules
from app.services.warehouse import (
    get_cached_state_payload,
    materialize_unit_payload,
    seed_layer_catalog,
    upsert_latest_state_cache,
)

try:
    import h3
except Exception:  # pragma: no cover
    h3 = None

try:
    from data_fetcher import run_pipeline as legacy_run_pipeline
    from data_fetcher import run_pipeline_batch as legacy_run_pipeline_batch
except Exception:  # pragma: no cover
    legacy_run_pipeline = None
    legacy_run_pipeline_batch = None


PIPELINE_LOCK = asyncio.Lock()
FIXED_CALIBRATION_POINTS = [
    (point["vv"], point["ndmi"])
    for point in DEFAULT_ALERT_RULESET["calibration"]["fixed_points"]
]

COVERAGE_APPLICABILITY = dict(DEFAULT_ALERT_RULESET["calibration"]["coverage_applicability"])

TEXTURE_OPTIONS = ["franca", "franco_arcillosa", "franco_arenosa", "arcillosa"]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clamp(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))


def _slugify(value: str) -> str:
    return (
        value.lower()
        .replace(" ", "-")
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )


def _seed(*parts: object) -> int:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _coverage_class_from_hash(seed_value: int) -> str:
    options = ["pastura_cultivo", "forestal", "humedal", "suelo_desnudo_urbano"]
    return options[seed_value % len(options)]


def _centroid_from_geojson(geojson: dict[str, Any] | None, fallback: tuple[float, float]) -> tuple[float, float]:
    if not geojson:
        return fallback
    geom = shape(geojson)
    centroid = geom.centroid
    return centroid.y, centroid.x


def _date_bounds(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _state_definitions_from_rules(rule_set: dict[str, Any] | None = None) -> dict[str, Any]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    return rules["states"]


STATE_DEFINITIONS = _state_definitions_from_rules()


def _state_from_risk(risk_score: float, rule_set: dict[str, Any] | None = None) -> str:
    definitions = _state_definitions_from_rules(rule_set)
    ordered = sorted(definitions.items(), key=lambda item: item[1]["level"])
    for state_name, definition in ordered:
        if risk_score <= float(definition["max_risk"]):
            return state_name
    return ordered[-1][0] if ordered else "Emergencia"


def _fixed_calibration_quantiles(fixed_points: list[dict[str, float]] | None = None) -> dict[str, Any]:
    source_points = fixed_points or DEFAULT_ALERT_RULESET["calibration"]["fixed_points"]
    vv_points = [point["vv"] for point in source_points]
    ndmi_points = [point["ndmi"] for point in source_points]
    labels = ["p10", "p25", "p50", "p75", "p90"]
    return {
        "vv": {label: value for label, value in zip(labels, vv_points, strict=True)},
        "ndmi": {label: value for label, value in zip(labels, ndmi_points, strict=True)},
    }


def _piecewise_interpolate(value: float | None, xs: list[float], ys: list[float]) -> float | None:
    if value is None:
        return None
    if value <= xs[0]:
        return ys[0]
    if value >= xs[-1]:
        return ys[-1]
    for left in range(len(xs) - 1):
        if xs[left] <= value <= xs[left + 1]:
            width = xs[left + 1] - xs[left]
            if width == 0:
                return ys[left]
            ratio = (value - xs[left]) / width
            return ys[left] + ratio * (ys[left + 1] - ys[left])
    return ys[-1]


def _estimate_ndmi_from_calibration(vv_db: float | None, calibration: CalibrationSnapshot | dict[str, Any]) -> float | None:
    quantiles = calibration.quantiles if isinstance(calibration, CalibrationSnapshot) else calibration["quantiles"]
    vv_q = quantiles["vv"]
    ndmi_q = quantiles["ndmi"]
    xs = [vv_q[key] for key in ("p10", "p25", "p50", "p75", "p90")]
    ys = [ndmi_q[key] for key in ("p10", "p25", "p50", "p75", "p90")]
    return _piecewise_interpolate(vv_db, xs, ys)


def _infer_soil_context(unit: AOIUnit, geojson: dict[str, Any] | None = None) -> dict[str, Any]:
    lat, lon = _centroid_from_geojson(geojson, (unit.centroid_lat or -32.0, unit.centroid_lon or -56.0))
    base_seed = _seed(unit.id, lat, lon, "soil")
    rng = random.Random(base_seed)

    slope_pct = round(1.5 + rng.random() * 7.0, 1)
    depth_cm = round(65 + rng.random() * 70, 1)
    awc_mm = round(90 + rng.random() * 140, 1)
    reserve_score = _clamp((awc_mm / 230.0) * 60 + (depth_cm / 140.0) * 25 + (12 - slope_pct) * 2.0)
    vulnerability_score = round(_clamp(100 - reserve_score), 1)
    coneat_group = 80 + (base_seed % 120)
    hydrologic_signal = "intermitente" if rng.random() < 0.35 else "estable"

    return {
        "source": "heuristic_public_fallback",
        "coneat_group": coneat_group,
        "texture": TEXTURE_OPTIONS[base_seed % len(TEXTURE_OPTIONS)],
        "effective_depth_cm": depth_cm,
        "slope_pct": slope_pct,
        "water_holding_capacity_mm": awc_mm,
        "reserve_potential_score": round(reserve_score, 1),
        "vulnerability_score": vulnerability_score,
        "apd_pad_available": False,
        "hydrologic_signal": hydrologic_signal,
        "lat": round(lat, 5),
        "lon": round(lon, 5),
    }


async def _fetch_forecast(lat: float, lon: float) -> list[dict[str, Any]]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": (
            "precipitation_sum,precipitation_probability_max,"
            "temperature_2m_max,temperature_2m_min,"
            "et0_fao_evapotranspiration,wind_speed_10m_max,"
            "wind_gusts_10m_max,wind_direction_10m_dominant"
        ),
        "hourly": "relative_humidity_2m",
        "forecast_days": 7,
        "timezone": settings.default_timezone,
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(settings.openmeteo_base_url, params=params)
            response.raise_for_status()
        payload = response.json()
        daily = payload.get("daily", {})
        hourly = payload.get("hourly", {})
        humidity_by_day: dict[str, list[float]] = defaultdict(list)
        for timestamp, humidity in zip(hourly.get("time", []), hourly.get("relative_humidity_2m", [])):
            if humidity is None:
                continue
            humidity_by_day[str(timestamp)[:10]].append(float(humidity))
        forecast = []
        for index, forecast_date in enumerate(daily.get("time", [])):
            humidity_values = humidity_by_day.get(str(forecast_date), [])
            forecast.append(
                {
                    "date": forecast_date,
                    "precip_mm": float(daily.get("precipitation_sum", [0] * 7)[index]),
                    "precip_probability_pct": float(daily.get("precipitation_probability_max", [0] * 7)[index]),
                    "et0_mm": float(daily.get("et0_fao_evapotranspiration", [0] * 7)[index]),
                    "temp_max_c": float(daily.get("temperature_2m_max", [0] * 7)[index]),
                    "temp_min_c": float(daily.get("temperature_2m_min", [0] * 7)[index]),
                    "wind_mps": float(daily.get("wind_speed_10m_max", [0] * 7)[index]) / 3.6,
                    "wind_gust_mps": float(daily.get("wind_gusts_10m_max", [0] * 7)[index]) / 3.6,
                    "wind_direction_deg": float(daily.get("wind_direction_10m_dominant", [0] * 7)[index]),
                    "humidity_mean_pct": round(mean(humidity_values), 1) if humidity_values else None,
                    "source": "openmeteo",
                }
            )
        return forecast
    except Exception:
        forecast = []
        base_seed = _seed(lat, lon, "forecast")
        rng = random.Random(base_seed)
        today = date.today()
        for offset in range(7):
            forecast.append(
                {
                    "date": str(today + timedelta(days=offset)),
                    "precip_mm": round(max(0.0, rng.uniform(0, 8) - offset * 0.3), 1),
                    "precip_probability_pct": round(_clamp(rng.uniform(12, 82), 0, 100), 0),
                    "et0_mm": round(rng.uniform(2.4, 6.1), 1),
                    "temp_max_c": round(rng.uniform(24, 36), 1),
                    "temp_min_c": round(rng.uniform(12, 21), 1),
                    "humidity_mean_pct": round(rng.uniform(44, 86), 0),
                    "wind_mps": round(rng.uniform(3.0, 8.0), 1),
                    "wind_gust_mps": round(rng.uniform(5.0, 12.0), 1),
                    "wind_direction_deg": round(rng.uniform(0, 359), 0),
                    "source": "synthetic_fallback",
                }
            )
        return forecast


def _forecast_pressure(
    forecast_days: list[dict[str, Any]],
    current_spi: float | None,
    rule_set: dict[str, Any] | None = None,
) -> tuple[float, list[dict[str, Any]], bool]:
    weather_rules = (rule_set or DEFAULT_ALERT_RULESET)["weather"]
    enriched: list[dict[str, Any]] = []
    expected_risks: list[float] = []
    improvement_signal = True
    spi_component = _clamp((abs(min(current_spi or 0.0, 0.0)) / max(weather_rules["spi_reference_abs"], 0.1)) * 100)
    for day in forecast_days:
        deficit = max(day.get("et0_mm", 0.0) - day.get("precip_mm", 0.0), 0.0)
        dryness = _clamp(
            deficit * weather_rules["deficit_multiplier"]
            + max(day.get("temp_max_c", 0.0) - weather_rules["temperature_reference_c"], 0.0) * weather_rules["temperature_multiplier"]
            + max(day.get("wind_mps", 0.0) - weather_rules["wind_reference_mps"], 0.0) * weather_rules["wind_multiplier"]
        )
        expected_risk = round(
            _clamp(
                dryness * (weather_rules["forecast_dryness_weight"] / 100.0)
                + spi_component * (weather_rules["forecast_spi_weight"] / 100.0)
            ),
            1,
        )
        if day.get("precip_mm", 0.0) > day.get("et0_mm", 0.0):
            expected_risk = round(_clamp(expected_risk - weather_rules["relief_bonus_if_precip_exceeds_et0"]), 1)
        if expected_risk > weather_rules["improvement_risk_threshold"]:
            improvement_signal = False
        enriched.append(
            {
                **day,
                "expected_risk": expected_risk,
                "escalation_reason": "deficit_hidrico_proyectado" if expected_risk >= 50 else "sin_escalamiento",
                "spi_trend": round(-spi_component / 100.0, 2),
            }
        )
        expected_risks.append(expected_risk)
    summary_days = max(int(weather_rules["forecast_days_summary"]), 1)
    return round(mean(expected_risks[:summary_days]), 1) if expected_risks else 0.0, enriched, improvement_signal


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _payload_is_current(payload: dict[str, Any] | None, target_date: date | None = None) -> bool:
    target_date = target_date or date.today()
    if not payload:
        return False
    observed_at = _parse_iso_datetime(payload.get("observed_at"))
    if observed_at is None:
        return False
    return observed_at.date() == target_date


def _with_cache_status(payload: dict[str, Any] | None, *, current: bool) -> dict[str, Any] | None:
    if payload is None:
        return None
    return {
        **payload,
        "cache_status": "current" if current else "stale",
        "served_from": "latest_state_cache",
    }


def _build_live_observation_from_payload(
    unit: AOIUnit,
    payload: dict[str, Any],
    *,
    geometry_source: str,
) -> dict[str, Any]:
    sentinel_1 = payload.get("sentinel_1", {})
    sentinel_2 = payload.get("sentinel_2", {})
    era5 = payload.get("era5", {})
    s1_observed_at = _parse_iso_datetime(sentinel_1.get("observed_at"))
    s2_observed_at = _parse_iso_datetime(sentinel_2.get("observed_at"))
    reference_times = [item for item in (s1_observed_at, s2_observed_at) if item is not None]
    latest_observed_at = max(reference_times) if reference_times else _now_utc()
    freshness_days = max((_now_utc() - latest_observed_at).total_seconds() / 86400.0, 0.0)
    lag_hours = 0.0
    if s1_observed_at and s2_observed_at:
        lag_hours = abs((s2_observed_at - s1_observed_at).total_seconds()) / 3600.0

    s2_valid_pct = float(sentinel_2.get("cobertura_pct") or 0.0)
    cloud_cover_pct = round(max(0.0, 100.0 - s2_valid_pct), 1)
    vegetation_mask = "vegetacion_densa" if (sentinel_2.get("ndmi_media") or 0.0) > 0.28 else "vegetacion_media"
    quality_score = round(
        _clamp(
            (s2_valid_pct * 0.45)
            + (100.0 - cloud_cover_pct) * 0.25
            + (100.0 if sentinel_1.get("vv_suelo_db_media") is not None else 35.0) * 0.20
            + (100.0 - min(lag_hours, 96.0)) * 0.10
        ),
        1,
    )

    return {
        "department": unit.department,
        "coverage_class": unit.coverage_class or "pastura_cultivo",
        "vegetation_mask": vegetation_mask,
        "source_mode": "live_copernicus",
        "s1_vv_db_mean": sentinel_1.get("vv_suelo_db_media"),
        "s1_humidity_mean_pct": sentinel_1.get("humedad_media"),
        "s1_pct_area_stressed": sentinel_1.get("pct_area_bajo_estres"),
        "s2_ndmi_mean": sentinel_2.get("ndmi_media"),
        "s2_valid_pct": s2_valid_pct,
        "cloud_cover_pct": cloud_cover_pct,
        "lag_hours": round(lag_hours, 1),
        "spi_30d": era5.get("spi_30d"),
        "spi_categoria": era5.get("spi_categoria"),
        "quality_score": quality_score,
        "quality_control": {
            "provider": "copernicus+openmeteo",
            "freshness_days": round(freshness_days, 2),
            "coverage_valid_pct": s2_valid_pct,
            "cloud_cover_pct": cloud_cover_pct,
            "lag_hours": round(lag_hours, 1),
            "geometry_source": geometry_source,
            "s1_observed_at": s1_observed_at.isoformat() if s1_observed_at else None,
            "s2_observed_at": s2_observed_at.isoformat() if s2_observed_at else None,
            "fallback_reason": None,
        },
        "raw_payload": payload,
    }


def _carry_forward_live_observation(
    unit: AOIUnit,
    target_date: date,
    recent_obs: list[SatelliteObservation],
    *,
    fallback_reason: str | None = None,
) -> dict[str, Any] | None:
    carry_forward_deadline = datetime.combine(target_date, time(hour=12), tzinfo=timezone.utc) - timedelta(days=settings.live_carry_forward_max_age_days)
    for observation in recent_obs:
        source_mode = observation.source_mode or ""
        if source_mode not in {"live_copernicus", "carry_forward_live"}:
            continue
        observed_at = observation.observed_at
        if observed_at is None:
            continue
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        else:
            observed_at = observed_at.astimezone(timezone.utc)
        if observed_at < carry_forward_deadline:
            continue

        quality_control = dict(observation.quality_control or {})
        freshness_days = max((datetime.combine(target_date, time(hour=12), tzinfo=timezone.utc) - observed_at).total_seconds() / 86400.0, 0.0)
        quality_control.update(
            {
                "provider": quality_control.get("provider", "carry_forward"),
                "freshness_days": round(freshness_days, 2),
                "fallback_reason": fallback_reason or "carry_forward_recent_live",
                "carry_forward_from": observed_at.isoformat(),
            }
        )
        return {
            "department": unit.department,
            "coverage_class": observation.coverage_class or unit.coverage_class or "pastura_cultivo",
            "vegetation_mask": observation.vegetation_mask or "vegetacion_media",
            "source_mode": "carry_forward_live",
            "s1_vv_db_mean": observation.s1_vv_db_mean,
            "s1_humidity_mean_pct": observation.s1_humidity_mean_pct,
            "s1_pct_area_stressed": observation.s1_pct_area_stressed,
            "s2_ndmi_mean": observation.s2_ndmi_mean,
            "s2_valid_pct": observation.s2_valid_pct,
            "cloud_cover_pct": observation.cloud_cover_pct,
            "lag_hours": observation.lag_hours,
            "spi_30d": observation.spi_30d,
            "spi_categoria": observation.spi_categoria,
            "quality_score": round(_clamp((observation.quality_score or 65.0) - freshness_days * 4.0), 1),
            "quality_control": quality_control,
            "raw_payload": {
                "carry_forward": True,
                "source_observed_at": observed_at.isoformat(),
                "source_mode": source_mode,
            },
        }
    return None


async def _prefetch_live_observations(units: list[AOIUnit], target_date: date) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if legacy_run_pipeline_batch is None or not settings.copernicus_enabled:
        return {}, {}

    areas = [
        {
            "unit_id": unit.id,
            "department": unit.department,
            "geom": unit.geometry_geojson,
            "lat": unit.centroid_lat,
            "lon": unit.centroid_lon,
            "geometry_source": unit.source or "catalog",
        }
        for unit in units
        if unit.geometry_geojson
    ]
    if not areas:
        return {}, {}

    results = await asyncio.to_thread(
        legacy_run_pipeline_batch,
        areas,
        settings.national_pipeline_live_workers,
        target_date,
    )
    unit_map = {unit.id: unit for unit in units}
    observations: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    for result in results:
        unit_id = result.get("unit_id")
        if unit_id not in unit_map:
            continue
        if result.get("status") == "ok":
            observations[unit_id] = _build_live_observation_from_payload(
                unit_map[unit_id],
                result["payload"],
                geometry_source=result.get("geometry_source", unit_map[unit_id].source or "catalog"),
            )
        else:
            errors[unit_id] = result.get("error", "live_batch_failed")
    return observations, errors


async def _try_live_observation(unit: AOIUnit, geojson: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if legacy_run_pipeline is None or not settings.copernicus_enabled:
        return None
    if geojson is None and unit.department.lower() != "rivera":
        return None
    try:
        payload = await asyncio.to_thread(legacy_run_pipeline, geojson)
    except Exception:
        return None
    return _build_live_observation_from_payload(unit, payload, geometry_source=unit.source or "drawn_polygon")


def _derive_productive_observation(
    unit: AOIUnit,
    department_unit: AOIUnit,
    department_state: AlertState,
    department_observation: SatelliteObservation | None,
    *,
    target_date: date,
    geojson: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_metrics = department_state.raw_metrics or {}
    unit_geometry = geojson or unit.geometry_geojson
    soil_context = _infer_soil_context(unit, unit_geometry)
    unit_category = str((unit.metadata_extra or {}).get("unit_category", "predio")).lower()
    category_multiplier = {
        "predio": 1.0,
        "potrero": 1.18,
        "lote": 1.08,
    }.get(unit_category, 1.0)

    area_proxy = 1.0
    if unit_geometry:
        try:
            area_proxy = max(math.sqrt(abs(shape(unit_geometry).area)) * 100.0, 1.0)
        except Exception:
            area_proxy = 1.0
    size_factor = _clamp(area_proxy, 1.0, 12.0) / 12.0

    rng = random.Random(_seed(unit.id, target_date.isoformat(), "productive-derived"))
    lat_gradient = (unit.centroid_lat or 0.0) - (department_unit.centroid_lat or 0.0)
    lon_gradient = (unit.centroid_lon or 0.0) - (department_unit.centroid_lon or 0.0)
    vulnerability_push = (soil_context.get("vulnerability_score", 50.0) - 50.0) / 50.0
    spatial_modifier = ((lat_gradient * 13.0) - (lon_gradient * 9.5) + rng.uniform(-0.9, 0.9)) * category_multiplier
    size_modifier = (0.5 - size_factor) * 5.2

    base_humidity = raw_metrics.get("s1_humidity_mean_pct")
    if base_humidity is None and department_observation is not None:
        base_humidity = department_observation.s1_humidity_mean_pct
    if base_humidity is None:
        base_humidity = 47.5
    humidity = round(_clamp(base_humidity - spatial_modifier * 4.5 - vulnerability_push * 8.0 - size_modifier, 2.0, 98.0), 1)

    base_ndmi = raw_metrics.get("s2_ndmi_mean")
    if base_ndmi is None:
        base_ndmi = raw_metrics.get("estimated_ndmi")
    if base_ndmi is None and department_observation is not None:
        base_ndmi = department_observation.s2_ndmi_mean
    if base_ndmi is None:
        base_ndmi = 0.03
    ndmi = round(max(-0.45, min(0.6, base_ndmi - spatial_modifier * 0.029 - vulnerability_push * 0.05 - size_modifier * 0.006)), 3)

    base_vv = raw_metrics.get("s1_vv_db_mean")
    if base_vv is None and department_observation is not None:
        base_vv = department_observation.s1_vv_db_mean
    if base_vv is None:
        base_vv = -12.0
    vv_db = round(max(-22.0, min(-4.0, base_vv + ((humidity - base_humidity) * 0.085) + rng.uniform(-0.4, 0.4))), 3)

    base_spi = raw_metrics.get("spi_30d")
    if base_spi is None and department_observation is not None:
        base_spi = department_observation.spi_30d
    if base_spi is None:
        base_spi = -0.5
    spi = round(max(-3.0, min(3.0, base_spi - lat_gradient * 0.18 + rng.uniform(-0.14, 0.14))), 3)

    department_qc = department_observation.quality_control if department_observation else {}
    valid_pct = department_observation.s2_valid_pct if department_observation and department_observation.s2_valid_pct is not None else 76.0
    cloud_pct = department_observation.cloud_cover_pct if department_observation and department_observation.cloud_cover_pct is not None else round(max(0.0, 100.0 - valid_pct), 1)
    lag_hours = department_observation.lag_hours if department_observation and department_observation.lag_hours is not None else 12.0
    quality_baseline = department_observation.quality_score if department_observation and department_observation.quality_score is not None else department_state.confidence_score
    quality_score = round(_clamp((quality_baseline or 68.0) - abs(size_modifier) * 0.8 + 2.5), 1)
    pct_stressed = round(_clamp(department_state.affected_pct + spatial_modifier * 4.2 + vulnerability_push * 9.0 + abs(size_modifier) * 2.0, 0.0, 100.0), 1)

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
            "provider": "derived_department_productive_overlay",
            "freshness_days": float(department_qc.get("freshness_days", 1.0)),
            "coverage_valid_pct": valid_pct,
            "cloud_cover_pct": cloud_pct,
            "lag_hours": lag_hours,
            "geometry_source": unit.source,
            "fallback_reason": "productive_unit_derived_from_department",
            "source_department": unit.department,
        },
        "raw_payload": {
            "source_department_unit": department_unit.id,
            "source_department_state": department_state.current_state,
            "unit_category": unit_category,
            "area_proxy": round(area_proxy, 3),
            "derivation_mode": "department_overlay",
        },
    }


async def _derive_productive_observation_from_department(
    session: AsyncSession,
    unit: AOIUnit,
    target_date: date,
    geojson: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if unit.unit_type != "productive_unit":
        return None

    target_boundary = datetime.combine(target_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    department_unit_result = await session.execute(
        select(AOIUnit).where(AOIUnit.unit_type == "department", AOIUnit.department == unit.department).limit(1)
    )
    department_unit = department_unit_result.scalar_one_or_none()
    if department_unit is None:
        return None

    department_state_result = await session.execute(
        select(AlertState)
        .where(AlertState.unit_id == department_unit.id, AlertState.observed_at < target_boundary)
        .order_by(desc(AlertState.observed_at))
        .limit(1)
    )
    department_state = department_state_result.scalar_one_or_none()
    if department_state is None:
        return None

    department_observation_result = await session.execute(
        select(SatelliteObservation)
        .where(SatelliteObservation.unit_id == department_unit.id, SatelliteObservation.observed_at < target_boundary)
        .order_by(desc(SatelliteObservation.observed_at))
        .limit(1)
    )
    department_observation = department_observation_result.scalar_one_or_none()
    return _derive_productive_observation(
        unit,
        department_unit,
        department_state,
        department_observation,
        target_date=target_date,
        geojson=geojson,
    )


def _simulate_observation(unit: AOIUnit, target_date: date, geojson: dict[str, Any] | None = None) -> dict[str, Any]:
    base_seed = _seed(unit.id, target_date.isoformat(), "observation")
    rng = random.Random(base_seed)
    doy = target_date.timetuple().tm_yday
    seasonal = (math.sin(((doy - 35) / 365.0) * 2 * math.pi) + 1) / 2
    climate_bias = 0.0
    for record in DEPARTMENTS:
        if record.name == unit.department:
            climate_bias = (record.lat + 35.0) * 1.8
            break

    humidity = round(_clamp(72 - seasonal * 34 + climate_bias + rng.uniform(-6, 6)), 1)
    ndmi = round(max(-0.45, min(0.55, (humidity - 45.0) / 90.0 + rng.uniform(-0.08, 0.08))), 3)
    vv_db = round(-17.8 + humidity / 9.0 + rng.uniform(-1.2, 1.2), 3)
    s2_valid_pct = round(_clamp(55 + rng.uniform(0, 40)), 1)
    cloud_cover_pct = round(_clamp(100 - s2_valid_pct), 1)
    pct_stressed = round(_clamp((55 - humidity) * 2.3 + rng.uniform(5, 20)), 1)
    spi_30d = round(max(-2.6, min(2.3, -1.2 + (1 - seasonal) * 1.9 + rng.uniform(-0.55, 0.55))), 3)
    vegetation_mask = "vegetacion_densa" if ndmi > 0.24 else "vegetacion_media"
    quality_score = round(_clamp(s2_valid_pct * 0.55 + (100 - cloud_cover_pct) * 0.25 + 20), 1)

    return {
        "department": unit.department,
        "coverage_class": unit.coverage_class or _coverage_class_from_hash(base_seed),
        "vegetation_mask": vegetation_mask,
        "source_mode": "simulated",
        "s1_vv_db_mean": vv_db,
        "s1_humidity_mean_pct": humidity,
        "s1_pct_area_stressed": pct_stressed,
        "s2_ndmi_mean": ndmi,
        "s2_valid_pct": s2_valid_pct,
        "cloud_cover_pct": cloud_cover_pct,
        "lag_hours": round(rng.uniform(18, 46), 1),
        "spi_30d": spi_30d,
        "spi_categoria": "moderadamente_seco" if spi_30d < -1 else "normal",
        "quality_score": quality_score,
        "quality_control": {
            "freshness_days": 1,
            "coverage_valid_pct": s2_valid_pct,
            "cloud_cover_pct": cloud_cover_pct,
            "lag_hours": round(rng.uniform(18, 46), 1),
            "fallback_reason": "synthetic_national_bootstrap",
        },
        "raw_payload": {"simulation_seed": base_seed, "geojson": geojson},
    }


async def _build_observation(
    session: AsyncSession,
    unit: AOIUnit,
    target_date: date,
    geojson: dict[str, Any] | None = None,
    *,
    recent_obs: list[SatelliteObservation] | None = None,
    prefetched_live: dict[str, Any] | None = None,
    prefetch_error: str | None = None,
) -> dict[str, Any]:
    if prefetched_live:
        return prefetched_live

    derived_productive = await _derive_productive_observation_from_department(session, unit, target_date, geojson)
    if derived_productive:
        return derived_productive

    live = await _try_live_observation(unit, geojson)
    if live:
        return live

    carry_forward = _carry_forward_live_observation(unit, target_date, recent_obs or [], fallback_reason=prefetch_error)
    if carry_forward:
        return carry_forward

    simulated = _simulate_observation(unit, target_date, geojson)
    if prefetch_error:
        simulated["quality_control"]["fallback_reason"] = prefetch_error[:180]
        simulated["quality_control"]["live_fetch_status"] = "failed"
        simulated["raw_payload"]["live_fetch_error"] = prefetch_error[:500]
    return simulated


async def _get_recent_observations(
    session: AsyncSession,
    *,
    department: str | None,
    coverage_class: str | None,
    vegetation_mask: str | None,
    since: datetime,
) -> list[SatelliteObservation]:
    query = select(SatelliteObservation).where(
        SatelliteObservation.observed_at >= since,
        SatelliteObservation.s1_vv_db_mean.is_not(None),
        SatelliteObservation.s2_ndmi_mean.is_not(None),
    )
    if department:
        query = query.where(SatelliteObservation.department == department)
    if coverage_class:
        query = query.where(SatelliteObservation.coverage_class == coverage_class)
    if vegetation_mask:
        query = query.where(SatelliteObservation.vegetation_mask == vegetation_mask)
    result = await session.execute(query.order_by(desc(SatelliteObservation.observed_at)))
    return list(result.scalars().all())


async def _persist_calibration(
    session: AsyncSession,
    *,
    department: str,
    coverage_class: str,
    vegetation_mask: str,
    generated_at: datetime,
    sample_count: int,
    fallback_level: str,
    quantiles: dict[str, Any],
    quality_score: float,
    window_days: int | None = None,
) -> CalibrationSnapshot:
    effective_window_days = int(window_days or DEFAULT_ALERT_RULESET["calibration"]["window_days"])
    snapshot = CalibrationSnapshot(
        department=department,
        coverage_class=coverage_class,
        vegetation_mask=vegetation_mask,
        generated_at=generated_at,
        window_start=generated_at - timedelta(days=effective_window_days),
        window_end=generated_at,
        sample_count=sample_count,
        fallback_level=fallback_level,
        quality_score=quality_score,
        quantiles=quantiles,
        coefficients={"method": "piecewise_quantiles"},
        metadata_extra={"version": "v1_nacional"},
    )
    session.add(snapshot)
    await session.flush()
    return snapshot


def _transient_calibration_snapshot(
    *,
    department: str,
    coverage_class: str,
    vegetation_mask: str,
    generated_at: datetime,
    sample_count: int,
    fallback_level: str,
    quantiles: dict[str, Any],
    quality_score: float,
) -> dict[str, Any]:
    return {
        "id": f"transient-{department}-{coverage_class}-{vegetation_mask}-{generated_at.date().isoformat()}",
        "department": department,
        "coverage_class": coverage_class,
        "vegetation_mask": vegetation_mask,
        "generated_at": generated_at.isoformat(),
        "sample_count": sample_count,
        "fallback_level": fallback_level,
        "quality_score": quality_score,
        "quantiles": quantiles,
    }


async def _resolve_calibration(
    session: AsyncSession,
    *,
    department: str,
    coverage_class: str,
    vegetation_mask: str,
    observed_at: datetime,
    rule_set: dict[str, Any] | None = None,
    persist: bool = True,
) -> CalibrationSnapshot | dict[str, Any]:
    calibration_rules = (rule_set or DEFAULT_ALERT_RULESET)["calibration"]
    since = observed_at - timedelta(days=int(calibration_rules["window_days"]))
    candidates = [
        ("department_class", department, coverage_class, None),
        ("department_mask", department, None, vegetation_mask),
        ("national", None, coverage_class, None),
    ]
    for fallback_level, dep, cov_class, mask in candidates:
        observations = await _get_recent_observations(
            session,
            department=dep,
            coverage_class=cov_class,
            vegetation_mask=mask,
            since=since,
        )
        if len(observations) < int(calibration_rules["min_samples"]):
            continue
        vv_values = np.array([obs.s1_vv_db_mean for obs in observations if obs.s1_vv_db_mean is not None], dtype=float)
        ndmi_values = np.array([obs.s2_ndmi_mean for obs in observations if obs.s2_ndmi_mean is not None], dtype=float)
        quantiles = {
            "vv": {
                "p10": round(float(np.quantile(vv_values, 0.10)), 4),
                "p25": round(float(np.quantile(vv_values, 0.25)), 4),
                "p50": round(float(np.quantile(vv_values, 0.50)), 4),
                "p75": round(float(np.quantile(vv_values, 0.75)), 4),
                "p90": round(float(np.quantile(vv_values, 0.90)), 4),
            },
            "ndmi": {
                "p10": round(float(np.quantile(ndmi_values, 0.10)), 4),
                "p25": round(float(np.quantile(ndmi_values, 0.25)), 4),
                "p50": round(float(np.quantile(ndmi_values, 0.50)), 4),
                "p75": round(float(np.quantile(ndmi_values, 0.75)), 4),
                "p90": round(float(np.quantile(ndmi_values, 0.90)), 4),
            },
        }
        quality_score = round(_clamp(45 + min(len(observations), 20) * 2.5 + float(np.std(ndmi_values)) * 40), 1)
        if not persist:
            return _transient_calibration_snapshot(
                department=department,
                coverage_class=coverage_class,
                vegetation_mask=vegetation_mask,
                generated_at=observed_at,
                sample_count=len(observations),
                fallback_level=fallback_level,
                quantiles=quantiles,
                quality_score=quality_score,
            )
        return await _persist_calibration(
            session,
            department=department,
            coverage_class=coverage_class,
            vegetation_mask=vegetation_mask,
            generated_at=observed_at,
            sample_count=len(observations),
            fallback_level=fallback_level,
            quantiles=quantiles,
            quality_score=quality_score,
            window_days=int(calibration_rules["window_days"]),
        )

    return {
        "id": calibration_rules["fallback_id"],
        "department": department,
        "coverage_class": coverage_class,
        "vegetation_mask": vegetation_mask,
        "generated_at": observed_at.isoformat(),
        "sample_count": 5,
        "fallback_level": "fixed",
        "quality_score": calibration_rules["fallback_quality_score"],
        "quantiles": _fixed_calibration_quantiles(calibration_rules["fixed_points"]),
    }


def _build_recent_summary(
    observation: dict[str, Any],
    history_events: list[AlertaEvento],
    history_obs: list[SatelliteObservation],
    rule_set: dict[str, Any] | None = None,
) -> tuple[float, float]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    persistence_rules = rules["persistence"]
    anomaly_rules = rules["anomaly"]
    consecutive = 1
    for event in history_events:
        if (event.risk_score or 0.0) >= persistence_rules["risk_threshold"]:
            consecutive += 1
        else:
            break

    anomaly = 0.0
    if history_obs:
        window_size = max(int(anomaly_rules["history_window_observations"]), 1)
        hist_h = [item.s1_humidity_mean_pct for item in history_obs if item.s1_humidity_mean_pct is not None][:window_size]
        hist_n = [item.s2_ndmi_mean for item in history_obs if item.s2_ndmi_mean is not None][:window_size]
        if hist_h:
            anomaly = max(
                anomaly,
                _clamp(((mean(hist_h) - (observation.get("s1_humidity_mean_pct") or 50.0)) / anomaly_rules["humidity_scale_pct"]) * 100),
            )
        if hist_n:
            anomaly = max(
                anomaly,
                _clamp(((mean(hist_n) - (observation.get("s2_ndmi_mean") or 0.0)) / anomaly_rules["ndmi_scale"]) * 100),
            )

    persistence = round(min(consecutive, int(persistence_rules["max_consecutive_events"])) * persistence_rules["points_per_event"], 1)
    return persistence, round(anomaly, 1)


def _ground_truth_component(ground_truth: list[GroundTruthMeasurement], observation: dict[str, Any]) -> float:
    if not ground_truth:
        return 30.0
    latest = ground_truth[0]
    if latest.soil_moisture_pct is None or observation.get("s1_humidity_mean_pct") is None:
        return 70.0
    delta = abs(latest.soil_moisture_pct - observation["s1_humidity_mean_pct"])
    return round(_clamp(100 - delta * 4.5), 1)


def _compute_scores(
    *,
    observation: dict[str, Any],
    calibration: CalibrationSnapshot | dict[str, Any],
    soil_context: dict[str, Any],
    forecast_days: list[dict[str, Any]],
    history_events: list[AlertaEvento],
    history_obs: list[SatelliteObservation],
    ground_truth: list[GroundTruthMeasurement],
    rule_set: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    magnitude_rules = rules["magnitude"]
    risk_weights = rules["risk_weights"]
    confidence_weights = rules["confidence_weights"]
    calibration_rules = rules["calibration"]
    confidence_rules = rules["confidence"]
    weather_rules = rules["weather"]
    estimated_ndmi = _estimate_ndmi_from_calibration(observation.get("s1_vv_db_mean"), calibration)
    humidity = observation.get("s1_humidity_mean_pct")
    ndmi = observation.get("s2_ndmi_mean")

    humidity_dryness = _clamp(((magnitude_rules["humidity_reference_pct"] - (humidity or magnitude_rules["humidity_reference_pct"])) / magnitude_rules["humidity_scale_pct"]) * 100)
    ndmi_reference = ndmi if ndmi is not None else (estimated_ndmi or 0.1)
    ndmi_dryness = _clamp(((magnitude_rules["ndmi_reference"] - ndmi_reference) / magnitude_rules["ndmi_scale"]) * 100)
    estimated_ndmi_dryness = _clamp(
        ((magnitude_rules["estimated_ndmi_reference"] - (estimated_ndmi or magnitude_rules["estimated_ndmi_reference"])) / magnitude_rules["estimated_ndmi_scale"]) * 100
    )
    magnitude = round(
        (magnitude_rules["humidity_weight"] / 100.0) * humidity_dryness
        + (magnitude_rules["ndmi_weight"] / 100.0) * max(ndmi_dryness, estimated_ndmi_dryness),
        1,
    )

    persistence, anomaly = _build_recent_summary(observation, history_events, history_obs, rules)
    forecast_pressure, enriched_forecast, forecast_improvement = _forecast_pressure(forecast_days, observation.get("spi_30d"), rules)
    weather_confirmation = round(
        _clamp(
            (_clamp(abs(min(observation.get("spi_30d") or 0.0, 0.0)) / weather_rules["spi_reference_abs"] * 100) * (weather_rules["spi_weight"] / 100.0))
            + forecast_pressure * (weather_rules["forecast_weight"] / 100.0)
        ),
        1,
    )
    soil_vulnerability = round(float(soil_context.get("vulnerability_score", 50.0)), 1)

    risk_score = round(
        (
            magnitude * risk_weights["magnitude"]
            + persistence * risk_weights["persistence"]
            + anomaly * risk_weights["anomaly"]
            + weather_confirmation * risk_weights["weather"]
            + soil_vulnerability * risk_weights["soil"]
        )
        / 100.0,
        1,
    )

    freshness_days = float(observation.get("quality_control", {}).get("freshness_days", 1.0))
    valid_pct = float(observation.get("s2_valid_pct") or 0.0)
    freshness_component = _clamp((100 - freshness_days * confidence_rules["freshness_penalty_per_day"]) * (valid_pct / 100.0))
    agreement_delta = abs((estimated_ndmi or ndmi or 0.0) - (ndmi if ndmi is not None else (estimated_ndmi or 0.0)))
    agreement_component = round(_clamp(100 - agreement_delta * confidence_rules["agreement_penalty_per_unit"]), 1)
    applicability_component = calibration_rules["coverage_applicability"].get(observation.get("coverage_class", "pastura_cultivo"), 60.0)
    calibration_quality = calibration.quality_score if isinstance(calibration, CalibrationSnapshot) else calibration["quality_score"]
    field_component = _ground_truth_component(ground_truth, observation)

    confidence_score = round(
        (
            freshness_component * confidence_weights["freshness"]
            + agreement_component * confidence_weights["agreement"]
            + applicability_component * confidence_weights["applicability"]
            + calibration_quality * confidence_weights["calibration"]
            + field_component * confidence_weights["ground_truth"]
        )
        / 100.0,
        1,
    )
    if observation.get("source_mode") == "simulated":
        confidence_score = round(_clamp(confidence_score - confidence_rules["simulated_penalty"]), 1)

    drivers = [
        {"name": "magnitud", "score": round(magnitude, 1), "detail": "stress observado por S1/S2"},
        {"name": "persistencia", "score": round(persistence, 1), "detail": "dias consecutivos en deterioro"},
        {"name": "anomalia_temporal", "score": round(anomaly, 1), "detail": "desvio contra serie reciente"},
        {"name": "confirmacion_meteorologica", "score": round(weather_confirmation, 1), "detail": "SPI y pronostico"},
        {"name": "vulnerabilidad_suelo", "score": round(soil_vulnerability, 1), "detail": "reserva potencial del suelo"},
    ]
    drivers.sort(key=lambda item: item["score"], reverse=True)

    return {
        "risk_score": risk_score,
        "confidence_score": confidence_score,
        "estimated_ndmi": estimated_ndmi,
        "forecast": enriched_forecast,
        "forecast_improvement": forecast_improvement,
        "drivers": drivers,
        "component_scores": {
            "magnitude": magnitude,
            "persistence": persistence,
            "anomaly": anomaly,
            "weather_confirmation": weather_confirmation,
            "soil_vulnerability": soil_vulnerability,
            "freshness": freshness_component,
            "agreement": agreement_component,
            "applicability": applicability_component,
            "calibration_quality": calibration_quality,
            "field_validation": field_component,
        },
    }


def _pseudo_hex_ids(unit_id: str, count: int = 7) -> list[str]:
    return [f"pseudo-{unit_id}-{index}" for index in range(count)]


def _hex_neighbors(hex_id: str, all_hexes: set[str]) -> set[str]:
    if h3 is not None:
        try:
            if hasattr(h3, "grid_disk"):
                return set(h3.grid_disk(hex_id, 1)) & all_hexes
            if hasattr(h3, "k_ring"):
                return set(h3.k_ring(hex_id, 1)) & all_hexes
        except Exception:
            pass
    if not hex_id.startswith("pseudo-"):
        return {hex_id}
    base, _, index = hex_id.rpartition("-")
    idx = int(index)
    neighbors = {hex_id}
    for candidate in (idx - 1, idx + 1):
        neighbor = f"{base}-{candidate}"
        if neighbor in all_hexes:
            neighbors.add(neighbor)
    return neighbors


def _geojson_to_hexes(geojson: dict[str, Any] | None, unit_id: str, rule_set: dict[str, Any] | None = None) -> list[str]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    resolution = int(rules["spatial"]["analytic_hex_resolution"])
    if geojson and h3 is not None:
        try:
            if hasattr(h3, "geo_to_cells"):
                cells = list(h3.geo_to_cells(geojson, resolution))
                if cells:
                    return cells
        except Exception:
            pass
    return _pseudo_hex_ids(unit_id)


def _summarize_spatial_risk(
    base_risk: float,
    persistence_days: int,
    unit_id: str,
    geojson: dict[str, Any] | None,
    rule_set: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    spatial_rules = rules["spatial"]
    hexes = _geojson_to_hexes(geojson, unit_id, rules)
    rng = random.Random(_seed(unit_id, "hex-surface"))
    metrics = []
    max_hexes = int(spatial_rules["max_hexes_evaluated"])
    for hex_id in hexes[:max_hexes]:
        metrics.append(
            {
                "hex_id": hex_id,
                "risk_score": round(_clamp(base_risk + rng.uniform(-16, 16)), 1),
                "persistence_days": max(1, persistence_days + rng.randint(-2, 2)),
            }
        )

    total = max(len(metrics), 1)
    affected = [item for item in metrics if item["risk_score"] >= spatial_rules["actionable_risk_threshold"]]
    affected_pct = round(len(affected) * 100.0 / total, 1)

    largest_cluster = 0
    stable_cluster = False
    visited: set[str] = set()
    all_hexes = {item["hex_id"] for item in metrics}
    metrics_map = {item["hex_id"]: item for item in metrics}

    for item in affected:
        hex_id = item["hex_id"]
        if hex_id in visited:
            continue
        queue = [hex_id]
        cluster: list[dict[str, Any]] = []
        while queue:
            current = queue.pop()
            if current in visited or current not in metrics_map or metrics_map[current]["risk_score"] < spatial_rules["actionable_risk_threshold"]:
                continue
            visited.add(current)
            cluster.append(metrics_map[current])
            for neighbor in _hex_neighbors(current, all_hexes):
                if neighbor not in visited:
                    queue.append(neighbor)
        largest_cluster = max(largest_cluster, len(cluster))
        if len(cluster) >= int(spatial_rules["stable_cluster_min_size"]) and mean(member["persistence_days"] for member in cluster) >= spatial_rules["stable_cluster_min_days"]:
            stable_cluster = True

    largest_cluster_pct = round(largest_cluster * 100.0 / total, 1)
    actionable = (
        affected_pct >= spatial_rules["affected_pct_threshold"]
        and largest_cluster_pct >= spatial_rules["largest_cluster_pct_threshold"]
    ) or stable_cluster
    return {
        "affected_pct": affected_pct,
        "largest_cluster_pct": largest_cluster_pct,
        "stable_cluster": stable_cluster,
        "actionable": actionable,
        "hex_count": total,
    }


def _apply_hysteresis(
    *,
    risk_score: float,
    confidence_score: float,
    previous_state: AlertState | None,
    recent_events: list[AlertaEvento],
    forecast_improvement: bool,
    rule_set: dict[str, Any] | None = None,
) -> tuple[str, int]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    definitions = _state_definitions_from_rules(rules)
    hysteresis_rules = rules["hysteresis"]
    raw_state = _state_from_risk(risk_score, rules)
    raw_level = definitions[raw_state]["level"]
    if previous_state is None:
        return raw_state, 1

    previous_level = previous_state.state_level
    previous_name = previous_state.current_state

    if raw_level > previous_level:
        if risk_score >= hysteresis_rules["emergency_jump_risk"] and confidence_score >= hysteresis_rules["emergency_jump_confidence"]:
            return "Emergencia", 1 if previous_name != "Emergencia" else previous_state.days_in_state + 1
        consecutive = 1
        threshold = definitions[raw_state]["min_risk"]
        for event in recent_events:
            if (event.risk_score or 0.0) >= threshold:
                consecutive += 1
            else:
                break
        if consecutive >= int(hysteresis_rules["raise_consecutive_observations"]):
            return raw_state, 1
        return previous_name, previous_state.days_in_state + 1

    if raw_level < previous_level:
        consecutive = 1
        threshold = definitions[previous_name]["exit_threshold"]
        for event in recent_events:
            if (event.risk_score or 0.0) < threshold:
                consecutive += 1
            else:
                break
        if consecutive >= int(hysteresis_rules["drop_consecutive_observations"]) and forecast_improvement:
            return raw_state, 1
        return previous_name, previous_state.days_in_state + 1

    return previous_name, previous_state.days_in_state + 1


async def _recent_ground_truth(session: AsyncSession, unit_id: str) -> list[GroundTruthMeasurement]:
    result = await session.execute(
        select(GroundTruthMeasurement)
        .where(
            GroundTruthMeasurement.unit_id == unit_id,
            GroundTruthMeasurement.observed_at >= _now_utc() - timedelta(days=7),
        )
        .order_by(desc(GroundTruthMeasurement.observed_at))
        .limit(5)
    )
    return list(result.scalars().all())


async def _load_recent_context(
    session: AsyncSession,
    unit_id: str,
    *,
    before_date: date | None = None,
) -> tuple[list[AlertaEvento], list[SatelliteObservation], AlertState | None]:
    boundary = None
    if before_date is not None:
        boundary = datetime.combine(before_date, time.min, tzinfo=timezone.utc)
    event_query = select(AlertaEvento).where(AlertaEvento.unit_id == unit_id)
    observation_query = select(SatelliteObservation).where(SatelliteObservation.unit_id == unit_id)
    if boundary is not None:
        event_query = event_query.where(AlertaEvento.fecha < boundary)
        observation_query = observation_query.where(SatelliteObservation.observed_at < boundary)
    events = await session.execute(
        event_query.order_by(desc(AlertaEvento.fecha)).limit(10)
    )
    observations = await session.execute(
        observation_query.order_by(desc(SatelliteObservation.observed_at)).limit(20)
    )
    events_rows = list(events.scalars().all())
    observation_rows = list(observations.scalars().all())
    current = await session.execute(select(AlertState).where(AlertState.unit_id == unit_id).limit(1))
    current_state = current.scalar_one_or_none()
    current_observed_at = current_state.observed_at if current_state is not None else None
    if current_observed_at is not None and current_observed_at.tzinfo is None:
        current_observed_at = current_observed_at.replace(tzinfo=timezone.utc)
    if boundary is not None and current_state is not None and current_observed_at is not None and current_observed_at >= boundary:
        current_state = None
        recent_event = next(iter(events_rows), None)
        if recent_event is not None:
            current_state = AlertState(
                unit_id=unit_id,
                scope=recent_event.scope,
                department=recent_event.departamento,
                observed_at=recent_event.fecha,
                current_state=recent_event.nivel_nombre,
                state_level=recent_event.nivel,
                risk_score=recent_event.risk_score or 0.0,
                confidence_score=recent_event.confidence_score or 0.0,
                affected_pct=recent_event.pct_area_afectada or 0.0,
                largest_cluster_pct=recent_event.largest_cluster_pct or 0.0,
                days_in_state=recent_event.days_in_state or 1,
                actionable=recent_event.actionable,
                data_mode=recent_event.tipo or "simulated",
                drivers=recent_event.drivers or [],
                forecast=recent_event.forecast or [],
                soil_context=recent_event.soil_context or {},
                calibration_ref=recent_event.calibration_ref,
                raw_metrics={},
                explanation=recent_event.descripcion,
                metadata_extra=recent_event.metadata_extra or {},
            )
    return (
        events_rows,
        observation_rows,
        current_state,
    )


async def _upsert_observation(
    session: AsyncSession,
    unit: AOIUnit,
    target_date: date,
    observation: dict[str, Any],
) -> SatelliteObservation:
    day_start, day_end = _date_bounds(target_date)
    result = await session.execute(
        select(SatelliteObservation)
        .where(
            SatelliteObservation.unit_id == unit.id,
            SatelliteObservation.observed_at >= day_start,
            SatelliteObservation.observed_at < day_end,
        )
        .limit(1)
    )
    record = result.scalar_one_or_none()
    if record is None:
        record = SatelliteObservation(
            unit_id=unit.id,
            department=unit.department,
            observed_at=day_start + timedelta(hours=12),
        )
        session.add(record)

    record.coverage_class = observation["coverage_class"]
    record.vegetation_mask = observation["vegetation_mask"]
    record.source_mode = observation["source_mode"]
    record.s1_vv_db_mean = observation.get("s1_vv_db_mean")
    record.s1_humidity_mean_pct = observation.get("s1_humidity_mean_pct")
    record.s1_pct_area_stressed = observation.get("s1_pct_area_stressed")
    record.s2_ndmi_mean = observation.get("s2_ndmi_mean")
    record.s2_valid_pct = observation.get("s2_valid_pct")
    record.cloud_cover_pct = observation.get("cloud_cover_pct")
    record.lag_hours = observation.get("lag_hours")
    record.spi_30d = observation.get("spi_30d")
    record.spi_categoria = observation.get("spi_categoria")
    record.quality_score = observation.get("quality_score", 0.0)
    record.quality_control = observation.get("quality_control", {})
    record.raw_payload = observation.get("raw_payload", {})
    await session.flush()
    return record


async def _upsert_ingestion_log(
    session: AsyncSession,
    unit: AOIUnit,
    target_date: date,
    observation: dict[str, Any],
) -> IngestionQualityLog:
    day_start, day_end = _date_bounds(target_date)
    result = await session.execute(
        select(IngestionQualityLog)
        .where(
            IngestionQualityLog.unit_id == unit.id,
            IngestionQualityLog.observed_at >= day_start,
            IngestionQualityLog.observed_at < day_end,
        )
        .limit(1)
    )
    record = result.scalar_one_or_none()
    if record is None:
        record = IngestionQualityLog(
            unit_id=unit.id,
            observed_at=day_start + timedelta(hours=12),
        )
        session.add(record)

    qc = observation.get("quality_control", {})
    record.source_mode = observation.get("source_mode", "simulated")
    record.provider = qc.get("provider", "copernicus+openmeteo")
    record.status = "fallback" if record.source_mode == "simulated" else ("carry_forward" if record.source_mode == "carry_forward_live" else "success")
    record.geometry_source = qc.get("geometry_source", unit.source)
    record.s1_observed_at = _parse_iso_datetime(qc.get("s1_observed_at"))
    record.s2_observed_at = _parse_iso_datetime(qc.get("s2_observed_at"))
    record.lag_hours = observation.get("lag_hours")
    record.valid_coverage_pct = observation.get("s2_valid_pct")
    record.cloud_cover_pct = observation.get("cloud_cover_pct")
    record.quality_score = observation.get("quality_score", 0.0)
    record.fallback_reason = qc.get("fallback_reason")
    record.payload = {
        "quality_control": qc,
        "raw_payload": observation.get("raw_payload", {}),
    }
    await session.flush()
    return record


async def _upsert_alert_state_and_event(
    session: AsyncSession,
    *,
    unit: AOIUnit,
    target_date: date,
    observation: dict[str, Any],
    calibration: CalibrationSnapshot | dict[str, Any],
    soil_context: dict[str, Any],
    score_payload: dict[str, Any],
    spatial_summary: dict[str, Any],
    state_name: str,
    days_in_state: int,
    rule_set: dict[str, Any] | None = None,
    rules_version: str | None = None,
    update_current_state: bool = True,
) -> tuple[AlertState, AlertaEvento]:
    rules = rule_set or DEFAULT_ALERT_RULESET
    definitions = _state_definitions_from_rules(rules)
    day_start, day_end = _date_bounds(target_date)
    level = definitions[state_name]["level"]
    definition = definitions[state_name]

    event_result = await session.execute(
        select(AlertaEvento)
        .where(AlertaEvento.unit_id == unit.id, AlertaEvento.fecha >= day_start, AlertaEvento.fecha < day_end)
        .limit(1)
    )
    event = event_result.scalar_one_or_none()
    if event is None:
        event = AlertaEvento(unit_id=unit.id, fecha=day_start + timedelta(hours=12), departamento=unit.department)
        session.add(event)

    event.geom_geojson = unit.geometry_geojson
    event.scope = unit.scope
    event.nivel = level
    event.nivel_nombre = state_name
    event.tipo = observation.get("source_mode")
    event.humedad_media_pct = observation.get("s1_humidity_mean_pct")
    event.ndmi_medio = observation.get("s2_ndmi_mean")
    event.spi_valor = observation.get("spi_30d")
    event.spi_categoria = observation.get("spi_categoria")
    event.pct_area_afectada = spatial_summary["affected_pct"]
    event.largest_cluster_pct = spatial_summary["largest_cluster_pct"]
    event.risk_score = score_payload["risk_score"]
    event.confidence_score = score_payload["confidence_score"]
    event.days_in_state = days_in_state
    event.actionable = spatial_summary["actionable"]
    event.es_prolongada = state_name != "Normal" and days_in_state >= 6
    event.drivers = score_payload["drivers"]
    event.forecast = score_payload["forecast"]
    event.soil_context = soil_context
    event.calibration_ref = calibration.id if isinstance(calibration, CalibrationSnapshot) else calibration["id"]
    event.descripcion = definition["description"]
    event.accion_recomendada = definition["action"]
    event.metadata_extra = {
        "data_mode": observation.get("source_mode"),
        "component_scores": score_payload["component_scores"],
        "estimated_ndmi": score_payload.get("estimated_ndmi"),
        "spatial_summary": spatial_summary,
        "quality_control": observation.get("quality_control", {}),
        "rules_version": rules_version,
    }

    state_values = {
        "scope": unit.scope,
        "department": unit.department,
        "observed_at": event.fecha,
        "current_state": state_name,
        "state_level": level,
        "risk_score": score_payload["risk_score"],
        "confidence_score": score_payload["confidence_score"],
        "affected_pct": spatial_summary["affected_pct"],
        "largest_cluster_pct": spatial_summary["largest_cluster_pct"],
        "days_in_state": days_in_state,
        "actionable": spatial_summary["actionable"],
        "data_mode": observation.get("source_mode", "simulated"),
        "drivers": score_payload["drivers"],
        "forecast": score_payload["forecast"],
        "soil_context": soil_context,
        "calibration_ref": event.calibration_ref,
        "raw_metrics": {
        "s1_vv_db_mean": observation.get("s1_vv_db_mean"),
        "s1_humidity_mean_pct": observation.get("s1_humidity_mean_pct"),
        "s2_ndmi_mean": observation.get("s2_ndmi_mean"),
        "spi_30d": observation.get("spi_30d"),
        "estimated_ndmi": score_payload.get("estimated_ndmi"),
        "component_scores": score_payload["component_scores"],
        "quality_control": observation.get("quality_control", {}),
        },
        "explanation": definition["description"],
        "metadata_extra": event.metadata_extra,
    }

    state_result = await session.execute(select(AlertState).where(AlertState.unit_id == unit.id).limit(1))
    current = state_result.scalar_one_or_none()
    previous_state_snapshot = None
    if current is not None:
        previous_state_snapshot = {
            "current_state": current.current_state,
            "state_level": current.state_level,
            "risk_score": current.risk_score,
            "confidence_score": current.confidence_score,
            "forecast": current.forecast or [],
            "affected_pct": current.affected_pct,
            "days_in_state": current.days_in_state,
        }
    state_snapshot = current
    if update_current_state:
        if current is None:
            current = AlertState(unit_id=unit.id)
            session.add(current)
        for field_name, field_value in state_values.items():
            setattr(current, field_name, field_value)
        state_snapshot = current
    else:
        state_snapshot = AlertState(unit_id=unit.id)
        for field_name, field_value in state_values.items():
            setattr(state_snapshot, field_name, field_value)

    humidity_result = await session.execute(
        select(HumedadSuelo)
        .where(HumedadSuelo.unit_id == unit.id, HumedadSuelo.fecha >= day_start, HumedadSuelo.fecha < day_end)
        .limit(1)
    )
    humidity_row = humidity_result.scalar_one_or_none()
    if humidity_row is None:
        humidity_row = HumedadSuelo(unit_id=unit.id, fecha=event.fecha)
        session.add(humidity_row)
    humidity_row.geom_geojson = unit.geometry_geojson
    humidity_row.humedad_s1_pct = observation.get("s1_humidity_mean_pct")
    humidity_row.ndmi_s2 = observation.get("s2_ndmi_mean")
    humidity_row.nivel_alerta = level
    humidity_row.cobertura_nubes_pct = observation.get("cloud_cover_pct")
    humidity_row.metadata_extra = {
        "source_mode": observation.get("source_mode"),
        "rules_version": rules_version,
    }
    await session.flush()
    if update_current_state:
        from app.services.notifications import notification_service

        await notification_service.dispatch_operational_alerts(
            session,
            unit=unit,
            alert_event=event,
            current_state=current,
            previous_state=previous_state_snapshot,
        )
    return state_snapshot, event


def _format_state_payload(unit: AOIUnit, state: AlertState, rule_set: dict[str, Any] | None = None) -> dict[str, Any]:
    definition = _state_definitions_from_rules(rule_set)[state.current_state]
    metadata_extra = unit.metadata_extra or {}
    return {
        "scope": state.scope,
        "unit_id": unit.id,
        "unit_name": unit.name,
        "department": state.department,
        "unit_type": unit.unit_type,
        "observed_at": state.observed_at.isoformat() if state.observed_at else None,
        "state": state.current_state,
        "state_level": state.state_level,
        "legacy_level": definition["legacy"],
        "color": definition["color"],
        "risk_score": round(state.risk_score or 0.0, 1),
        "confidence_score": round(state.confidence_score or 0.0, 1),
        "affected_pct": round(state.affected_pct or 0.0, 1),
        "largest_cluster_pct": round(state.largest_cluster_pct or 0.0, 1),
        "days_in_state": state.days_in_state,
        "actionable": state.actionable,
        "drivers": state.drivers or [],
        "forecast": state.forecast or [],
        "soil_context": state.soil_context or {},
        "calibration_ref": state.calibration_ref,
        "data_mode": state.data_mode,
        "geometry_source": unit.source,
        "unit_category": metadata_extra.get("unit_category"),
        "source_name": metadata_extra.get("source_name"),
        "external_id": metadata_extra.get("external_id"),
        "h3_index": metadata_extra.get("h3_index"),
        "h3_resolution": metadata_extra.get("h3_resolution"),
        "fallback_role": metadata_extra.get("fallback_role"),
        "explanation": state.explanation,
        "raw_metrics": state.raw_metrics or {},
        "rules_version": (state.metadata_extra or {}).get("rules_version"),
    }


def _format_legacy_payload(event: AlertaEvento, observation: SatelliteObservation | None) -> dict[str, Any]:
    definition = STATE_DEFINITIONS[event.nivel_nombre]
    observation_payload = observation.raw_payload if observation else {}
    sentinel_1_raw = observation_payload.get("sentinel_1", {}) if isinstance(observation_payload, dict) else {}
    sentinel_2_raw = observation_payload.get("sentinel_2", {}) if isinstance(observation_payload, dict) else {}
    era5_raw = observation_payload.get("era5", {}) if isinstance(observation_payload, dict) else {}

    return {
        "fecha": event.fecha.date().isoformat(),
        "departamento": event.departamento,
        "alerta": {
            "nivel": definition["legacy"],
            "codigo": event.nivel,
            "color": definition["color"],
            "descripcion": event.descripcion,
            "accion": event.accion_recomendada,
        },
        "sentinel_1": {
            "vv_db_media": observation.s1_vv_db_mean if observation else None,
            "humedad_media": event.humedad_media_pct,
            "humedad_p10": sentinel_1_raw.get("humedad_p10"),
            "humedad_p90": sentinel_1_raw.get("humedad_p90"),
            "pct_area_bajo_estres": event.pct_area_afectada,
            "cobertura_pct": observation.s2_valid_pct if observation else None,
        },
        "sentinel_2": {
            "ndmi_media": event.ndmi_medio,
            "ndmi_p10": sentinel_2_raw.get("ndmi_p10"),
            "ndmi_p90": sentinel_2_raw.get("ndmi_p90"),
            "cobertura_pct": observation.s2_valid_pct if observation else None,
        },
        "era5": {
            "spi_30d": event.spi_valor,
            "spi_categoria": event.spi_categoria,
            **(era5_raw if isinstance(era5_raw, dict) else {}),
        },
        "resumen": {
            "nivel": definition["legacy"],
            "color": definition["color"],
            "humedad_s1_pct": event.humedad_media_pct,
            "ndmi_s2": event.ndmi_medio,
            "spi_30d": event.spi_valor,
            "spi_categoria": event.spi_categoria,
        },
        "dias_deficit": event.days_in_state if event.nivel >= 1 else 0,
        "es_prolongada": event.es_prolongada,
        "advertencia": "Datos simulados" if observation and observation.source_mode == "simulated" else None,
    }


async def analyze_unit(
    session: AsyncSession,
    *,
    unit: AOIUnit,
    target_date: date,
    geojson: dict[str, Any] | None = None,
    observation_payload: dict[str, Any] | None = None,
    prefetch_error: str | None = None,
    update_current_state: bool = True,
) -> dict[str, Any]:
    recent_events, recent_obs, previous_state = await _load_recent_context(session, unit.id, before_date=target_date)
    resolved_rules = await get_effective_alert_rules(session, unit.coverage_class)
    rule_set = resolved_rules["rules"]
    rules_version = resolved_rules["rules_version"]
    observation_payload = observation_payload or await _build_observation(
        session,
        unit,
        target_date,
        geojson,
        recent_obs=recent_obs,
        prefetch_error=prefetch_error,
    )
    observation_record = await _upsert_observation(session, unit, target_date, observation_payload)
    await _upsert_ingestion_log(session, unit, target_date, observation_payload)
    if update_current_state:
        unit.data_mode = observation_payload.get("source_mode", unit.data_mode)
    calibration = await _resolve_calibration(
        session,
        department=unit.department,
        coverage_class=observation_record.coverage_class,
        vegetation_mask=observation_record.vegetation_mask,
        observed_at=observation_record.observed_at,
        rule_set=rule_set,
    )
    soil_context = _infer_soil_context(unit, geojson)
    forecast = []
    if target_date >= date.today():
        forecast = await _fetch_forecast(
            unit.centroid_lat or soil_context["lat"],
            unit.centroid_lon or soil_context["lon"],
        )
    ground_truth = await _recent_ground_truth(session, unit.id)
    score_payload = _compute_scores(
        observation=observation_payload,
        calibration=calibration,
        soil_context=soil_context,
        forecast_days=forecast,
        history_events=recent_events,
        history_obs=recent_obs,
        ground_truth=ground_truth,
        rule_set=rule_set,
    )
    spatial_summary = _summarize_spatial_risk(
        score_payload["risk_score"],
        previous_state.days_in_state if previous_state else 1,
        unit.id,
        geojson or unit.geometry_geojson,
        rule_set,
    )
    state_name, days_in_state = _apply_hysteresis(
        risk_score=score_payload["risk_score"],
        confidence_score=score_payload["confidence_score"],
        previous_state=previous_state,
        recent_events=recent_events,
        forecast_improvement=score_payload["forecast_improvement"],
        rule_set=rule_set,
    )
    current_state, event = await _upsert_alert_state_and_event(
        session,
        unit=unit,
        target_date=target_date,
        observation=observation_payload,
        calibration=calibration,
        soil_context=soil_context,
        score_payload=score_payload,
        spatial_summary=spatial_summary,
        state_name=state_name,
        days_in_state=days_in_state,
        rule_set=rule_set,
        rules_version=rules_version,
        update_current_state=update_current_state,
    )
    if update_current_state:
        forecast_window_end = datetime.combine(target_date + timedelta(days=8), time.min, tzinfo=timezone.utc)
        existing_forecasts = await session.execute(
            select(ForecastSignal).where(
                ForecastSignal.unit_id == unit.id,
                ForecastSignal.forecast_date >= datetime.combine(target_date, time.min, tzinfo=timezone.utc),
                ForecastSignal.forecast_date < forecast_window_end,
            )
        )
        for row in existing_forecasts.scalars().all():
            await session.delete(row)
        for day in score_payload["forecast"]:
            session.add(
                ForecastSignal(
                    unit_id=unit.id,
                    forecast_date=datetime.fromisoformat(day["date"]).replace(tzinfo=timezone.utc),
                    precip_mm=day.get("precip_mm"),
                    et0_mm=day.get("et0_mm"),
                    temp_max_c=day.get("temp_max_c"),
                    wind_mps=day.get("wind_mps"),
                    spi_trend=day.get("spi_trend"),
                    expected_risk=day.get("expected_risk"),
                    escalation_reason=day.get("escalation_reason"),
                    payload=day,
                )
            )
    await session.commit()
    return {"unit": unit, "state": current_state, "event": event, "observation": observation_record}


async def _current_analysis_status(session: AsyncSession, target_date: date) -> dict[str, Any]:
    day_start, day_end = _date_bounds(target_date)
    count_result = await session.execute(
        select(func.count()).select_from(AlertState).where(AlertState.scope == "departamento", AlertState.observed_at >= day_start, AlertState.observed_at < day_end)
    )
    current_count = count_result.scalar_one()
    modes_result = await session.execute(
        select(AlertState.data_mode).where(AlertState.scope == "departamento", AlertState.observed_at >= day_start, AlertState.observed_at < day_end)
    )
    modes = [mode for mode in modes_result.scalars().all() if mode]
    live_units = sum(1 for mode in modes if mode == "live_copernicus")
    carry_forward_units = sum(1 for mode in modes if mode == "carry_forward_live")
    simulated_units = sum(1 for mode in modes if mode == "simulated")
    return {
        "target_date": str(target_date),
        "units": current_count,
        "live_units": live_units,
        "carry_forward_units": carry_forward_units,
        "simulated_units": simulated_units,
        "ready": current_count >= len(DEPARTMENTS),
        "needs_live_refresh": settings.copernicus_enabled and current_count >= len(DEPARTMENTS) and live_units == 0 and carry_forward_units == 0,
    }


async def ensure_latest_daily_analysis(session: AsyncSession, target_date: date | None = None) -> dict[str, Any]:
    target_date = target_date or date.today()
    await seed_catalog_units(session)
    status = await _current_analysis_status(session, target_date)
    if status["ready"] and not status["needs_live_refresh"]:
        return {**status, "status": "already_current"}

    async with PIPELINE_LOCK:
        status = await _current_analysis_status(session, target_date)
        if status["ready"] and not status["needs_live_refresh"]:
            return {**status, "status": "already_current"}
        return await run_daily_pipeline(session, target_date=target_date)


async def run_daily_pipeline(
    session: AsyncSession,
    target_date: date | None = None,
    *,
    update_current_state: bool | None = None,
    materialize_latest: bool | None = None,
    refresh_catalog_geometries: bool = True,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    is_current_target = target_date >= date.today()
    if update_current_state is None:
        update_current_state = is_current_target
    if materialize_latest is None:
        materialize_latest = is_current_target

    await seed_catalog_units(session, refresh_geometries=refresh_catalog_geometries)
    await seed_layer_catalog(session)
    result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "department").order_by(AOIUnit.department))
    units = list(result.scalars().all())
    prefetched_live, live_errors = await _prefetch_live_observations(units, target_date)
    processed = []
    formatted_payloads: list[dict[str, Any]] = []
    for unit in units:
        payload = await analyze_unit(
            session,
            unit=unit,
            target_date=target_date,
            observation_payload=prefetched_live.get(unit.id),
            prefetch_error=live_errors.get(unit.id),
            update_current_state=update_current_state,
        )
        processed.append(
            {
                "unit_id": unit.id,
                "department": unit.department,
                "state": payload["state"].current_state,
                "risk_score": payload["state"].risk_score,
                "data_mode": payload["observation"].source_mode,
                "fallback_reason": (payload["observation"].quality_control or {}).get("fallback_reason"),
            }
        )
        formatted = _format_state_payload(unit, payload["state"])
        formatted_payloads.append(formatted)
        await materialize_unit_payload(
            session,
            unit,
            formatted,
            update_latest_cache=materialize_latest,
            update_spatial_features=materialize_latest,
        )

    national_payload = _aggregate_states(formatted_payloads)
    previous_national_payload = await get_cached_state_payload(session, scope="nacional", department="Uruguay")
    if materialize_latest:
        await upsert_latest_state_cache(session, national_payload, scope="nacional", department="Uruguay")
        from app.services.notifications import notification_service

        await notification_service.dispatch_national_alert_subscriptions(
            session,
            current_payload=national_payload,
            previous_payload=previous_national_payload,
        )

    from app.services.sections import materialize_police_section_cache

    section_result = {"count": 0}
    hex_result = {"count": 0}
    if materialize_latest:
        section_result = await materialize_police_section_cache(
            session,
            target_date=target_date,
            ensure_base_analysis=False,
            persist_latest=True,
        )
        from app.services.hexagons import materialize_h3_cache

        hex_result = await materialize_h3_cache(
            session,
            target_date=target_date,
            ensure_base_analysis=False,
            persist_latest=True,
        )
    productive_result = {"count": 0}
    from app.services.productive_units import materialize_productive_unit_cache

    productive_result = await materialize_productive_unit_cache(
        session,
        target_date=target_date,
        ensure_base_analysis=False,
        persist_latest=materialize_latest,
    )
    await session.commit()
    live_count = sum(1 for item in processed if item["data_mode"] == "live_copernicus")
    carry_forward_count = sum(1 for item in processed if item["data_mode"] == "carry_forward_live")
    simulated_count = sum(1 for item in processed if item["data_mode"] == "simulated")
    return {
        "target_date": str(target_date),
        "processed": len(processed),
        "live_count": live_count,
        "carry_forward_count": carry_forward_count,
        "simulated_count": simulated_count,
        "units": processed,
        "section_cache_count": section_result.get("count", 0),
        "hex_cache_count": hex_result.get("count", 0),
        "productive_cache_count": productive_result.get("count", 0),
    }


async def backfill_department_spatial_cache(session: AsyncSession, department: str | None = None) -> dict[str, Any]:
    query = select(AOIUnit).where(AOIUnit.unit_type == "department").order_by(AOIUnit.department)
    if department:
        query = query.where(AOIUnit.department == department)
    result = await session.execute(query)
    units = list(result.scalars().all())

    state_result = await session.execute(select(AlertState).order_by(desc(AlertState.observed_at)))
    states: dict[str, AlertState] = {}
    for state in state_result.scalars().all():
        states.setdefault(state.unit_id, state)

    count = 0
    for unit in units:
        if not unit.geometry_geojson:
            continue
        payload = await get_cached_state_payload(session, scope=unit.scope, unit_id=unit.id, department=unit.department)
        if payload is None:
            state = states.get(unit.id)
            if state is None:
                continue
            payload = _format_state_payload(unit, state)
        await materialize_unit_payload(session, unit, payload)
        count += 1

    await session.flush()
    return {"count": count, "department_filter": department}


async def recompute_calibrations(session: AsyncSession, as_of: date | None = None) -> dict[str, Any]:
    as_of = as_of or date.today()
    result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "department"))
    units = list(result.scalars().all())
    created = []
    for unit in units:
        resolved_rules = await get_effective_alert_rules(session, unit.coverage_class)
        snapshot = await _resolve_calibration(
            session,
            department=unit.department,
            coverage_class=unit.coverage_class,
            vegetation_mask="vegetacion_media",
            observed_at=datetime.combine(as_of, time(hour=3, minute=30), tzinfo=timezone.utc),
            rule_set=resolved_rules["rules"],
        )
        created.append({"department": unit.department, "coverage_class": unit.coverage_class, "calibration_ref": snapshot.id if isinstance(snapshot, CalibrationSnapshot) else snapshot["id"], "fallback_level": snapshot.fallback_level if isinstance(snapshot, CalibrationSnapshot) else snapshot["fallback_level"]})
    await session.commit()
    return {"target_date": str(as_of), "calibrations": created}


async def _get_unit(session: AsyncSession, *, unit_id: str | None = None, department: str | None = None) -> AOIUnit | None:
    if unit_id:
        result = await session.execute(select(AOIUnit).where(AOIUnit.id == unit_id).limit(1))
        return result.scalar_one_or_none()
    if department:
        result = await session.execute(select(AOIUnit).where(AOIUnit.slug == f"departamento-{_slugify(department)}").limit(1))
        return result.scalar_one_or_none()
    return None


def _aggregate_states(payloads: list[dict[str, Any]], rule_set: dict[str, Any] | None = None) -> dict[str, Any]:
    definitions = _state_definitions_from_rules(rule_set)
    if not payloads:
        definition = definitions["Normal"]
        return {"scope": "nacional", "unit_id": "nacional", "unit_name": "Uruguay", "department": "Uruguay", "observed_at": None, "state": "Normal", "state_level": 0, "legacy_level": definition["legacy"], "color": definition["color"], "risk_score": 0.0, "confidence_score": 0.0, "affected_pct": 0.0, "largest_cluster_pct": 0.0, "days_in_state": 0, "actionable": False, "drivers": [], "forecast": [], "soil_context": {}, "calibration_ref": "multiple", "data_mode": "simulated", "explanation": definition["description"], "raw_metrics": {}}

    avg_risk = round(mean(item["risk_score"] for item in payloads), 1)
    max_risk = round(max(item["risk_score"] for item in payloads), 1)
    avg_confidence = round(mean(item["confidence_score"] for item in payloads), 1)
    max_level = max(item["state_level"] for item in payloads)
    state_name = next(name for name, definition in definitions.items() if definition["level"] == max_level)
    definition = definitions[state_name]
    mode_counter = Counter(item.get("data_mode", "simulated") for item in payloads)
    drivers_counter = Counter()
    driver_scores: defaultdict[str, list[float]] = defaultdict(list)
    component_scores: defaultdict[str, list[float]] = defaultdict(list)
    forecast_by_day: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    vv_values: list[float] = []
    humidity_values: list[float] = []
    ndmi_values: list[float] = []
    spi_values: list[float] = []
    estimated_ndmi_values: list[float] = []
    for payload in payloads:
        for driver in payload["drivers"]:
            drivers_counter[driver["name"]] += 1
            driver_scores[driver["name"]].append(driver["score"])
        for forecast in payload["forecast"]:
            forecast_by_day[forecast["date"]].append(forecast)
        raw_metrics = payload.get("raw_metrics") or {}
        if raw_metrics.get("s1_vv_db_mean") is not None:
            vv_values.append(raw_metrics["s1_vv_db_mean"])
        if raw_metrics.get("s1_humidity_mean_pct") is not None:
            humidity_values.append(raw_metrics["s1_humidity_mean_pct"])
        if raw_metrics.get("s2_ndmi_mean") is not None:
            ndmi_values.append(raw_metrics["s2_ndmi_mean"])
        if raw_metrics.get("spi_30d") is not None:
            spi_values.append(raw_metrics["spi_30d"])
        if raw_metrics.get("estimated_ndmi") is not None:
            estimated_ndmi_values.append(raw_metrics["estimated_ndmi"])
        for name, score in (raw_metrics.get("component_scores") or {}).items():
            if score is not None:
                component_scores[name].append(score)
    drivers = [{"name": name, "score": round(mean(driver_scores[name]), 1), "detail": f"presente en {drivers_counter[name]} departamentos"} for name, _ in drivers_counter.most_common(5)]
    forecast = [{"date": forecast_date, "expected_risk": round(mean(item["expected_risk"] for item in entries), 1), "precip_mm": round(mean(item["precip_mm"] for item in entries), 1), "et0_mm": round(mean(item["et0_mm"] for item in entries), 1), "temp_max_c": round(mean(item["temp_max_c"] for item in entries), 1), "wind_mps": round(mean(item["wind_mps"] for item in entries), 1), "escalation_reason": "resumen_nacional"} for forecast_date, entries in sorted(forecast_by_day.items())[:7]]
    top_risk_departments = sorted(payloads, key=lambda item: item["risk_score"], reverse=True)[:5]
    rules_versions = {item.get("rules_version") for item in payloads if item.get("rules_version")}
    return {
        "scope": "nacional",
        "unit_id": "nacional",
        "unit_name": "Uruguay",
        "department": "Uruguay",
        "observed_at": payloads[0]["observed_at"],
        "state": state_name,
        "state_level": max_level,
        "legacy_level": definition["legacy"],
        "color": definition["color"],
        "risk_score": max_risk,
        "confidence_score": avg_confidence,
        "affected_pct": round(mean(item["affected_pct"] for item in payloads), 1),
        "largest_cluster_pct": round(max(item["largest_cluster_pct"] for item in payloads), 1),
        "days_in_state": max(item["days_in_state"] for item in payloads),
        "actionable": any(item["actionable"] for item in payloads),
        "drivers": drivers,
        "forecast": forecast,
        "soil_context": {"source": "aggregated", "department_count": len(payloads)},
        "calibration_ref": "multiple",
        "data_mode": next(iter(mode_counter)) if len(mode_counter) == 1 else "mixed",
        "explanation": definition["description"],
        "rules_version": next(iter(rules_versions)) if len(rules_versions) == 1 else ("mixed" if rules_versions else None),
        "raw_metrics": {
            "s1_vv_db_mean": round(mean(vv_values), 3) if vv_values else None,
            "s1_humidity_mean_pct": round(mean(humidity_values), 1) if humidity_values else None,
            "s2_ndmi_mean": round(mean(ndmi_values), 3) if ndmi_values else None,
            "spi_30d": round(mean(spi_values), 3) if spi_values else None,
            "estimated_ndmi": round(mean(estimated_ndmi_values), 3) if estimated_ndmi_values else None,
            "component_scores": {name: round(mean(scores), 1) for name, scores in component_scores.items() if scores},
            "top_risk_departments": top_risk_departments,
        },
    }


def _history_window_dates(limit: int, *, end_date: date | None = None) -> list[date]:
    end_date = end_date or date.today()
    span = max(1, limit)
    start_date = end_date - timedelta(days=span - 1)
    return [start_date + timedelta(days=offset) for offset in range(span)]


def _history_window_bounds(limit: int, *, end_date: date | None = None) -> tuple[datetime, datetime, list[date]]:
    window_dates = _history_window_dates(limit, end_date=end_date)
    start_at = datetime.combine(window_dates[0], time.min, tzinfo=timezone.utc)
    end_at = datetime.combine(window_dates[-1] + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return start_at, end_at, window_dates


def _event_date(observed_at: datetime) -> date:
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.astimezone(timezone.utc).date()


def _event_datetime(observed_at: datetime) -> datetime:
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.astimezone(timezone.utc)


async def _missing_department_history_by_date(
    session: AsyncSession,
    *,
    units: list[AOIUnit],
    limit: int,
) -> dict[date, list[AOIUnit]]:
    if not units:
        return {}
    start_at, end_at, window_dates = _history_window_bounds(limit)
    result = await session.execute(
        select(AlertaEvento.unit_id, AlertaEvento.fecha).where(
            AlertaEvento.unit_id.in_([unit.id for unit in units]),
            AlertaEvento.fecha >= start_at,
            AlertaEvento.fecha < end_at,
        )
    )
    existing_dates_by_unit: defaultdict[str, set[date]] = defaultdict(set)
    for unit_ref, observed_at in result.all():
        if observed_at is None:
            continue
        existing_dates_by_unit[unit_ref].add(_event_date(observed_at))

    missing_by_date: dict[date, list[AOIUnit]] = {}
    for target_date in window_dates:
        missing_units = [unit for unit in units if target_date not in existing_dates_by_unit.get(unit.id, set())]
        if missing_units:
            missing_by_date[target_date] = missing_units
    return missing_by_date


async def _ensure_recent_department_event_history(
    session: AsyncSession,
    *,
    limit: int,
    department: str | None = None,
) -> dict[str, Any]:
    await seed_catalog_units(session, refresh_geometries=False)
    query = select(AOIUnit).where(AOIUnit.unit_type == "department").order_by(AOIUnit.department)
    if department:
        query = query.where(AOIUnit.department == department)
    result = await session.execute(query)
    units = list(result.scalars().all())
    if not units:
        return {"filled_dates": 0, "filled_unit_days": 0, "department": department}

    start_at, end_at, _ = _history_window_bounds(limit)
    events_result = await session.execute(
        select(AlertaEvento)
        .where(
            AlertaEvento.unit_id.in_([unit.id for unit in units]),
            AlertaEvento.fecha >= start_at,
            AlertaEvento.fecha < end_at,
        )
        .order_by(AlertaEvento.fecha)
    )
    existing_events = list(events_result.scalars().all())

    async with PIPELINE_LOCK:
        missing_by_date = await _missing_department_history_by_date(session, units=units, limit=limit)
        if not missing_by_date:
            return {
                "filled_dates": 0,
                "filled_unit_days": 0,
                "department": department,
                "synthetic_events_by_unit": {},
                "synthetic_events": [],
            }

        rules_cache: dict[str, dict[str, Any]] = {}
        existing_by_unit: defaultdict[str, list[AlertaEvento]] = defaultdict(list)
        for event in existing_events:
            existing_by_unit[event.unit_id].append(event)

        synthetic_events_by_unit: defaultdict[str, list[AlertaEvento]] = defaultdict(list)
        filled_dates = 0
        filled_unit_days = 0
        for target_date, missing_units in sorted(missing_by_date.items()):
            for unit in missing_units:
                coverage_class = unit.coverage_class or "pastura_cultivo"
                resolved_rules = rules_cache.get(coverage_class)
                if resolved_rules is None:
                    resolved_rules = await get_effective_alert_rules(session, coverage_class)
                    rules_cache[coverage_class] = resolved_rules
                rule_set = resolved_rules["rules"]
                calibration_rules = rule_set["calibration"]
                observation = _simulate_observation(unit, target_date, unit.geometry_geojson)
                observation.setdefault("quality_control", {})
                observation["quality_control"]["fallback_reason"] = "recent_history_bootstrap"
                observation["quality_control"]["backfill_mode"] = "lightweight_simulated"
                calibration = {
                    "id": calibration_rules["fallback_id"],
                    "department": unit.department,
                    "coverage_class": coverage_class,
                    "vegetation_mask": observation.get("vegetation_mask", "vegetacion_media"),
                    "generated_at": datetime.combine(target_date, time(hour=12), tzinfo=timezone.utc).isoformat(),
                    "sample_count": 5,
                    "fallback_level": "fixed",
                    "quality_score": calibration_rules["fallback_quality_score"],
                    "quantiles": _fixed_calibration_quantiles(calibration_rules["fixed_points"]),
                }
                prior_events = [
                    item
                    for item in existing_by_unit.get(unit.id, [])
                    if _event_date(item.fecha) < target_date
                ]
                recent_events = sorted(prior_events, key=lambda item: _event_datetime(item.fecha), reverse=True)
                previous_event = max(prior_events, key=lambda item: _event_datetime(item.fecha), default=None)
                previous_state = None
                if previous_event is not None:
                    previous_state = AlertState(
                        unit_id=unit.id,
                        current_state=previous_event.nivel_nombre or "Normal",
                        state_level=previous_event.nivel or 0,
                        days_in_state=previous_event.days_in_state or 1,
                        forecast=previous_event.forecast or [],
                        affected_pct=previous_event.pct_area_afectada or 0.0,
                        risk_score=previous_event.risk_score or 0.0,
                        confidence_score=previous_event.confidence_score or 0.0,
                    )
                soil_context = _infer_soil_context(unit, unit.geometry_geojson)
                score_payload = _compute_scores(
                    observation=observation,
                    calibration=calibration,
                    soil_context=soil_context,
                    forecast_days=[],
                    history_events=recent_events,
                    history_obs=[],
                    ground_truth=[],
                    rule_set=rule_set,
                )
                spatial_summary = _summarize_spatial_risk(
                    score_payload["risk_score"],
                    previous_state.days_in_state if previous_state else 1,
                    unit.id,
                    None,
                    rule_set,
                )
                state_name, days_in_state = _apply_hysteresis(
                    risk_score=score_payload["risk_score"],
                    confidence_score=score_payload["confidence_score"],
                    previous_state=previous_state,
                    recent_events=recent_events,
                    forecast_improvement=False,
                    rule_set=rule_set,
                )
                level = _state_definitions_from_rules(rule_set)[state_name]["level"]
                synthetic_event = AlertaEvento(
                    unit_id=unit.id,
                    fecha=datetime.combine(target_date, time(hour=12), tzinfo=timezone.utc),
                    departamento=unit.department,
                    scope="departamento",
                    nivel=level,
                    nivel_nombre=state_name,
                    tipo=observation.get("source_mode"),
                    humedad_media_pct=observation.get("s1_humidity_mean_pct"),
                    ndmi_medio=observation.get("s2_ndmi_mean"),
                    spi_valor=observation.get("spi_30d"),
                    spi_categoria=observation.get("spi_categoria"),
                    pct_area_afectada=spatial_summary["affected_pct"],
                    largest_cluster_pct=spatial_summary["largest_cluster_pct"],
                    risk_score=score_payload["risk_score"],
                    confidence_score=score_payload["confidence_score"],
                    days_in_state=days_in_state,
                    actionable=spatial_summary["actionable"],
                    drivers=score_payload["drivers"],
                    forecast=[],
                    soil_context=soil_context,
                    calibration_ref=calibration["id"],
                    metadata_extra={
                        "bootstrap": True,
                        "bootstrap_mode": "lightweight_simulated",
                        "rules_version": resolved_rules["rules_version"],
                    },
                )
                existing_by_unit[unit.id].append(synthetic_event)
                synthetic_events_by_unit[unit.id].append(synthetic_event)
                filled_unit_days += 1
            filled_dates += 1
        synthetic_events = [event for events in synthetic_events_by_unit.values() for event in events]
        return {
            "filled_dates": filled_dates,
            "filled_unit_days": filled_unit_days,
            "department": department,
            "synthetic_events_by_unit": {unit_ref: list(events) for unit_ref, events in synthetic_events_by_unit.items()},
            "synthetic_events": synthetic_events,
        }


async def get_scope_snapshot(session: AsyncSession, *, scope: str = "departamento", unit_id: str | None = None, department: str | None = None) -> dict[str, Any]:
    if scope == "nacional":
        cached = await get_cached_state_payload(session, scope="nacional", department="Uruguay")
        if _payload_is_current(cached):
            return _with_cache_status(cached, current=True)
        if cached:
            return _with_cache_status(cached, current=False)
        await ensure_latest_daily_analysis(session)
        cached = await get_cached_state_payload(session, scope="nacional", department="Uruguay")
        if cached:
            return _with_cache_status(cached, current=_payload_is_current(cached))
        states_result = await session.execute(select(AlertState).order_by(desc(AlertState.risk_score)))
        states = list(states_result.scalars().all())
        units_result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "department"))
        units = {unit.id: unit for unit in units_result.scalars().all()}
        payload = _aggregate_states([_format_state_payload(units[state.unit_id], state) for state in states if state.unit_id in units])
        await upsert_latest_state_cache(session, payload, scope="nacional", department="Uruguay")
        await session.commit()
        return _with_cache_status(payload, current=_payload_is_current(payload))

    unit = await _get_unit(session, unit_id=unit_id, department=department or settings.aoi_department)
    if unit is None:
        raise ValueError("Unidad no encontrada")
    cached = await get_cached_state_payload(session, scope=unit.scope, unit_id=unit.id, department=unit.department)
    if _payload_is_current(cached):
        return _with_cache_status(cached, current=True)
    if cached:
        return _with_cache_status(cached, current=False)

    if unit.unit_type == "h3_cell":
        from app.services.hexagons import materialize_h3_cache

        await materialize_h3_cache(session, department=unit.department)
    elif unit.unit_type == "productive_unit":
        from app.services.productive_units import materialize_productive_unit_cache

        await materialize_productive_unit_cache(
            session,
            target_date=date.today(),
            unit_ids=[unit.id],
            ensure_base_analysis=True,
            persist_latest=True,
        )
    elif unit.scope == "seccion":
        from app.services.sections import materialize_police_section_cache

        await materialize_police_section_cache(session, department=unit.department)
    else:
        await ensure_latest_daily_analysis(session)
    cached = await get_cached_state_payload(session, scope=unit.scope, unit_id=unit.id, department=unit.department)
    if cached:
        return _with_cache_status(cached, current=_payload_is_current(cached))
    state_result = await session.execute(select(AlertState).where(AlertState.unit_id == unit.id).limit(1))
    state = state_result.scalar_one_or_none()
    refresh_target = unit.unit_type in {"custom_hex", "police_section", "h3_cell"}
    current_date = date.today()
    is_current = False
    if state and state.observed_at:
        observed_at = state.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        is_current = observed_at.astimezone(timezone.utc).date() == current_date
    if state is None or (refresh_target and not is_current):
        state = (await analyze_unit(session, unit=unit, target_date=current_date, geojson=unit.geometry_geojson))["state"]
    payload = _format_state_payload(unit, state)
    await upsert_latest_state_cache(session, payload, scope=payload.get("scope", unit.scope), unit_id=unit.id, department=unit.department)
    await session.commit()
    return _with_cache_status(payload, current=_payload_is_current(payload))


async def get_scope_weather_forecast(
    session: AsyncSession,
    *,
    scope: str = "departamento",
    unit_id: str | None = None,
    department: str | None = None,
) -> dict[str, Any]:
    if scope == "nacional":
        await seed_catalog_units(session)
        units_result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "department"))
        units = list(units_result.scalars().all())
        lat_values = [unit.centroid_lat for unit in units if unit.centroid_lat is not None]
        lon_values = [unit.centroid_lon for unit in units if unit.centroid_lon is not None]
        lat = round(mean(lat_values), 5) if lat_values else -32.5
        lon = round(mean(lon_values), 5) if lon_values else -56.0
        label = "Uruguay"
        cached_payload = await get_cached_state_payload(session, scope="nacional", department="Uruguay")
        spi_reference = (cached_payload or {}).get("raw_metrics", {}).get("spi_30d")
        effective_scope = "nacional"
        effective_department = "Uruguay"
        effective_unit_id = None
    else:
        unit = await _get_unit(session, unit_id=unit_id, department=department or settings.aoi_department)
        if unit is None:
            raise ValueError("Unidad no encontrada")
        lat = unit.centroid_lat or -32.5
        lon = unit.centroid_lon or -56.0
        label = unit.name
        cached_payload = await get_cached_state_payload(session, scope=unit.scope, unit_id=unit.id, department=unit.department)
        spi_reference = (cached_payload or {}).get("raw_metrics", {}).get("spi_30d")
        effective_scope = unit.scope
        effective_department = unit.department
        effective_unit_id = unit.id

    forecast = await _fetch_forecast(lat, lon)
    _, enriched, improvement_signal = _forecast_pressure(forecast, spi_reference)
    return {
        "scope": effective_scope,
        "unit_id": effective_unit_id,
        "department": effective_department,
        "selection_label": label,
        "lat": lat,
        "lon": lon,
        "spi_reference": spi_reference,
        "improvement_signal": improvement_signal,
        "source": "live_openmeteo",
        "forecast": enriched,
    }


async def get_alert_history(session: AsyncSession, *, scope: str = "departamento", unit_id: str | None = None, department: str | None = None, limit: int = 30) -> dict[str, Any]:
    window_start, window_end, window_dates = _history_window_bounds(limit)
    if scope == "nacional":
        global_rules = (await get_effective_alert_rules(session))["rules"]
        result = await session.execute(
            select(AlertaEvento)
            .where(
                AlertaEvento.scope == "departamento",
                AlertaEvento.fecha >= window_start,
                AlertaEvento.fecha < window_end,
            )
            .order_by(desc(AlertaEvento.fecha))
            .limit(limit * len(DEPARTMENTS))
        )
        events = list(result.scalars().all())
        grouped: defaultdict[str, list[AlertaEvento]] = defaultdict(list)
        for event in events:
            grouped[event.fecha.date().isoformat()].append(event)
        if len(grouped) < len(window_dates):
            bootstrap = await _ensure_recent_department_event_history(session, limit=limit)
            events.extend(bootstrap.get("synthetic_events", []))
            grouped = defaultdict(list)
            for event in events:
                grouped[event.fecha.date().isoformat()].append(event)
        data = []
        for event_date, rows in sorted(grouped.items(), reverse=True)[:limit]:
            avg_risk = round(mean(item.risk_score or 0.0 for item in rows), 1)
            data.append(
                {
                    "fecha": event_date,
                    "state": _state_from_risk(avg_risk, global_rules),
                    "risk_score": avg_risk,
                    "confidence_score": round(mean(item.confidence_score or 0.0 for item in rows), 1),
                    "affected_pct": round(mean(item.pct_area_afectada or 0.0 for item in rows), 1),
                }
            )
        return {"scope": "nacional", "total": len(data), "datos": data}

    unit = await _get_unit(session, unit_id=unit_id, department=department or settings.aoi_department)
    if unit is None:
        raise ValueError("Unidad no encontrada")
    result = await session.execute(
        select(AlertaEvento)
        .where(
            AlertaEvento.unit_id == unit.id,
            AlertaEvento.fecha >= window_start,
            AlertaEvento.fecha < window_end,
        )
        .order_by(desc(AlertaEvento.fecha))
        .limit(limit)
    )
    events = list(result.scalars().all())
    if unit.unit_type == "department" and len(events) < len(window_dates):
        bootstrap = await _ensure_recent_department_event_history(session, limit=limit, department=unit.department)
        events.extend(bootstrap.get("synthetic_events_by_unit", {}).get(unit.id, []))
    events = sorted(events, key=lambda item: _event_datetime(item.fecha), reverse=True)[:limit]
    if not events:
        snapshot_result = await session.execute(
            select(UnitIndexSnapshot)
            .where(UnitIndexSnapshot.unit_id == unit.id)
            .order_by(desc(UnitIndexSnapshot.observed_at))
            .limit(limit)
        )
        snapshots = list(snapshot_result.scalars().all())
        if not snapshots:
            if unit.unit_type == "h3_cell":
                from app.services.hexagons import materialize_h3_cache

                await materialize_h3_cache(session, department=unit.department)
            elif unit.unit_type == "productive_unit":
                from app.services.productive_units import materialize_productive_unit_cache

                await materialize_productive_unit_cache(
                    session,
                    target_date=date.today(),
                    unit_ids=[unit.id],
                    ensure_base_analysis=True,
                    persist_latest=True,
                )
            elif unit.scope == "seccion":
                from app.services.sections import materialize_police_section_cache

                await materialize_police_section_cache(session, department=unit.department)
            else:
                await ensure_latest_daily_analysis(session)
            snapshot_result = await session.execute(
                select(UnitIndexSnapshot)
                .where(UnitIndexSnapshot.unit_id == unit.id)
                .order_by(desc(UnitIndexSnapshot.observed_at))
                .limit(limit)
            )
            snapshots = list(snapshot_result.scalars().all())
        return {
            "scope": unit.scope,
            "unit_id": unit.id,
            "unit_name": unit.name,
            "total": len(snapshots),
            "datos": [
                {
                    "fecha": snapshot.observed_at.date().isoformat(),
                    "state": snapshot.state,
                    "state_level": snapshot.state_level,
                    "risk_score": round(snapshot.risk_score or 0.0, 1),
                    "confidence_score": round(snapshot.confidence_score or 0.0, 1),
                    "affected_pct": round(snapshot.affected_pct or 0.0, 1),
                    "largest_cluster_pct": round(snapshot.largest_cluster_pct or 0.0, 1),
                    "drivers": snapshot.drivers or [],
                }
                for snapshot in snapshots
            ],
        }
    return {"scope": unit.scope, "unit_id": unit.id, "unit_name": unit.name, "total": len(events), "datos": [{"fecha": event.fecha.date().isoformat(), "state": event.nivel_nombre, "state_level": event.nivel, "risk_score": round(event.risk_score or 0.0, 1), "confidence_score": round(event.confidence_score or 0.0, 1), "affected_pct": round(event.pct_area_afectada or 0.0, 1), "largest_cluster_pct": round(event.largest_cluster_pct or 0.0, 1), "drivers": event.drivers or []} for event in events]}


async def get_legacy_state(session: AsyncSession, *, department: str = "Rivera") -> dict[str, Any]:
    await ensure_latest_daily_analysis(session)
    unit = await _get_unit(session, department=department)
    if unit is None:
        raise ValueError("Departamento no encontrado")
    event_result = await session.execute(select(AlertaEvento).where(AlertaEvento.unit_id == unit.id).order_by(desc(AlertaEvento.fecha)).limit(1))
    observation_result = await session.execute(select(SatelliteObservation).where(SatelliteObservation.unit_id == unit.id).order_by(desc(SatelliteObservation.observed_at)).limit(1))
    event = event_result.scalar_one_or_none()
    observation = observation_result.scalar_one_or_none()
    if event is None:
        payload = await analyze_unit(session, unit=unit, target_date=date.today())
        event = payload["event"]
        observation = payload["observation"]
    return _format_legacy_payload(event, observation)


async def get_legacy_history(session: AsyncSession, *, department: str = "Rivera", days: int = 30) -> dict[str, Any]:
    unit = await _get_unit(session, department=department)
    if unit is None:
        raise ValueError("Departamento no encontrado")
    result = await session.execute(select(AlertaEvento).where(AlertaEvento.unit_id == unit.id).order_by(desc(AlertaEvento.fecha)).limit(days))
    events = list(result.scalars().all())
    return {"departamento": department, "datos": [{"fecha": event.fecha.date().isoformat(), "nivel": event.nivel, "humedad_pct": event.humedad_media_pct or 0.0, "ndmi": event.ndmi_medio or 0.0} for event in events]}


async def get_unit_traceability(session: AsyncSession, unit_id: str) -> dict[str, Any]:
    unit = await _get_unit(session, unit_id=unit_id)
    if unit is None:
        raise ValueError("Unidad no encontrada")
    events = await get_alert_history(session, scope="unidad", unit_id=unit_id, limit=15)
    calibration_result = await session.execute(select(CalibrationSnapshot).where(CalibrationSnapshot.department == unit.department).order_by(desc(CalibrationSnapshot.generated_at)).limit(5))
    gt_result = await session.execute(select(GroundTruthMeasurement).where(GroundTruthMeasurement.unit_id == unit.id).order_by(desc(GroundTruthMeasurement.observed_at)).limit(5))
    ingestion_result = await session.execute(select(IngestionQualityLog).where(IngestionQualityLog.unit_id == unit.id).order_by(desc(IngestionQualityLog.observed_at)).limit(5))
    return {
        "unit": {
            "id": unit.id,
            "name": unit.name,
            "department": unit.department,
            "scope": unit.scope,
            "unit_category": (unit.metadata_extra or {}).get("unit_category"),
            "coverage_class": unit.coverage_class,
            "geometry_geojson": unit.geometry_geojson,
            "geometry_source": unit.source,
        },
        "history": events["datos"],
        "calibrations": [{"id": snapshot.id, "generated_at": snapshot.generated_at.isoformat(), "fallback_level": snapshot.fallback_level, "quality_score": snapshot.quality_score, "sample_count": snapshot.sample_count} for snapshot in calibration_result.scalars().all()],
        "ground_truth": [{"id": measurement.id, "observed_at": measurement.observed_at.isoformat(), "source_type": measurement.source_type, "soil_moisture_pct": measurement.soil_moisture_pct, "pasture_condition": measurement.pasture_condition, "confidence": measurement.confidence} for measurement in gt_result.scalars().all()],
        "ingestion": [
            {
                "observed_at": item.observed_at.isoformat(),
                "source_mode": item.source_mode,
                "status": item.status,
                "geometry_source": item.geometry_source,
                "quality_score": item.quality_score,
                "fallback_reason": item.fallback_reason,
                "lag_hours": item.lag_hours,
                "valid_coverage_pct": item.valid_coverage_pct,
                "cloud_cover_pct": item.cloud_cover_pct,
            }
            for item in ingestion_result.scalars().all()
        ],
    }


async def list_units(
    session: AsyncSession,
    include_custom: bool = False,
    include_hex: bool = False,
    include_productive: bool = False,
) -> list[dict[str, Any]]:
    query = select(AOIUnit).order_by(AOIUnit.unit_type, AOIUnit.department, AOIUnit.name)
    if not include_custom:
        allowed_types = ["department"]
        if include_hex:
            allowed_types.append("h3_cell")
        if include_productive:
            allowed_types.append("productive_unit")
        query = query.where(AOIUnit.unit_type.in_(allowed_types))
    result = await session.execute(query)
    units = list(result.scalars().all())
    cache_result = await session.execute(select(UnitIndexSnapshot).order_by(desc(UnitIndexSnapshot.observed_at)))
    latest_snapshots: dict[str, UnitIndexSnapshot] = {}
    for snapshot in cache_result.scalars().all():
        latest_snapshots.setdefault(snapshot.unit_id, snapshot)
    if units and not latest_snapshots:
        await ensure_latest_daily_analysis(session)
        cache_result = await session.execute(select(UnitIndexSnapshot).order_by(desc(UnitIndexSnapshot.observed_at)))
        latest_snapshots = {}
        for snapshot in cache_result.scalars().all():
            latest_snapshots.setdefault(snapshot.unit_id, snapshot)
    if include_productive:
        missing_productive_ids = [unit.id for unit in units if unit.unit_type == "productive_unit" and unit.id not in latest_snapshots]
        if missing_productive_ids:
            from app.services.productive_units import materialize_productive_unit_cache

            await materialize_productive_unit_cache(
                session,
                target_date=date.today(),
                unit_ids=missing_productive_ids,
                ensure_base_analysis=True,
                persist_latest=True,
            )
            cache_result = await session.execute(select(UnitIndexSnapshot).order_by(desc(UnitIndexSnapshot.observed_at)))
            latest_snapshots = {}
            for snapshot in cache_result.scalars().all():
                latest_snapshots.setdefault(snapshot.unit_id, snapshot)
    state_result = await session.execute(select(AlertState))
    states = {state.unit_id: state for state in state_result.scalars().all()}
    return [
        {
            "id": unit.id,
            "slug": unit.slug,
            "name": unit.name,
            "department": unit.department,
            "unit_type": unit.unit_type,
            "scope": unit.scope,
            "centroid_lat": unit.centroid_lat,
            "centroid_lon": unit.centroid_lon,
            "coverage_class": unit.coverage_class,
            "unit_category": (unit.metadata_extra or {}).get("unit_category"),
            "geometry_source": unit.source,
            "h3_index": (unit.metadata_extra or {}).get("h3_index"),
            "h3_resolution": (unit.metadata_extra or {}).get("h3_resolution"),
            "data_mode": (latest_snapshots[unit.id].data_mode if unit.id in latest_snapshots else unit.data_mode),
            "state": (latest_snapshots[unit.id].state if unit.id in latest_snapshots else (states[unit.id].current_state if unit.id in states else None)),
            "risk_score": (round(latest_snapshots[unit.id].risk_score, 1) if unit.id in latest_snapshots else (round(states[unit.id].risk_score, 1) if unit.id in states else None)),
            "confidence_score": (round(latest_snapshots[unit.id].confidence_score, 1) if unit.id in latest_snapshots else (round(states[unit.id].confidence_score, 1) if unit.id in states else None)),
        }
        for unit in units
    ]


async def get_or_create_custom_unit(session: AsyncSession, geojson: dict[str, Any], name: str = "Mi Parcela") -> AOIUnit:
    lat, lon = _centroid_from_geojson(geojson, (-32.0, -56.0))
    unit_seed = hashlib.sha1(f"{geojson}".encode("utf-8")).hexdigest()[:12]
    unit_id = f"custom-r9-{unit_seed}"
    result = await session.execute(select(AOIUnit).where(AOIUnit.id == unit_id).limit(1))
    unit = result.scalar_one_or_none()
    if unit:
        return unit
    unit = AOIUnit(id=unit_id, slug=unit_id, unit_type="custom_hex", scope="unidad", name=name, department="Custom", geometry_geojson=geojson, centroid_lat=lat, centroid_lon=lon, coverage_class=_coverage_class_from_hash(_seed(unit_id)), source="drawn_polygon", data_mode="simulated", metadata_extra={"hex_resolution": settings.default_hex_resolution})
    session.add(unit)
    await session.commit()
    return unit


async def analyze_custom_geojson(session: AsyncSession, geojson: dict[str, Any], name: str = "Mi Parcela") -> dict[str, Any]:
    unit = await get_or_create_custom_unit(session, geojson, name)
    payload = await analyze_unit(session, unit=unit, target_date=date.today(), geojson=geojson)
    return _format_legacy_payload(payload["event"], payload["observation"])


async def ingest_ground_truth_measurement(session: AsyncSession, payload: dict[str, Any]) -> dict[str, Any]:
    unit_id = payload.get("unit_id")
    if unit_id is None and payload.get("geometry_geojson"):
        unit = await get_or_create_custom_unit(session, payload["geometry_geojson"], "Observacion de Campo")
        unit_id = unit.id
    observed_at = datetime.fromisoformat(payload["observed_at"])
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    else:
        observed_at = observed_at.astimezone(timezone.utc)
    measurement = GroundTruthMeasurement(
        unit_id=unit_id,
        observed_at=observed_at,
        source_type=payload["source_type"],
        sensor_id=payload.get("sensor_id"),
        soil_moisture_pct=payload.get("soil_moisture_pct"),
        pasture_condition=payload.get("pasture_condition"),
        vegetation_condition=payload.get("vegetation_condition"),
        confidence=payload.get("confidence", 70.0),
        notes=payload.get("notes"),
        geometry_geojson=payload.get("geometry_geojson"),
        raw_payload=payload.get("raw_payload", payload),
    )
    session.add(measurement)
    await session.commit()
    return {"id": measurement.id, "unit_id": measurement.unit_id, "observed_at": measurement.observed_at.isoformat(), "source_type": measurement.source_type, "status": "stored"}
