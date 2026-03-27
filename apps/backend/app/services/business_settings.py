from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.humedad import AOIUnit, SatelliteObservation
from app.models.settings import BusinessSettingsAudit, BusinessSettingsProfile


COVERAGE_OPTIONS = [
    {"key": "pastura_cultivo", "label": "Pastura / cultivo"},
    {"key": "forestal", "label": "Forestal"},
    {"key": "humedal", "label": "Humedal"},
    {"key": "suelo_desnudo_urbano", "label": "Suelo desnudo / urbano"},
]
COVERAGE_CLASSES = [item["key"] for item in COVERAGE_OPTIONS]
RECENT_RECALCULATION_DAYS = 30

_SETTINGS_CACHE: dict[str, Any] | None = None
_LAST_RECALCULATION_STATUS: dict[str, Any] = {
    "status": "idle",
    "window_days": RECENT_RECALCULATION_DAYS,
    "updated_at": None,
    "processed_units": 0,
    "processed_days": 0,
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fixed_points() -> list[dict[str, float]]:
    return [
        {"label": "p10", "vv": -16.92, "ndmi": -0.33},
        {"label": "p25", "vv": -13.49, "ndmi": -0.11},
        {"label": "p50", "vv": -12.42, "ndmi": 0.07},
        {"label": "p75", "vv": -10.96, "ndmi": 0.25},
        {"label": "p90", "vv": -8.97, "ndmi": 0.44},
    ]


def _build_default_ruleset() -> dict[str, Any]:
    return {
        "states": {
            "Normal": {
                "level": 0,
                "min_risk": 0.0,
                "max_risk": 24.0,
                "exit_threshold": 0.0,
                "legacy": "VERDE",
                "color": "#2ecc71",
                "description": "Condicion hidrica estable y sin evidencia consistente de estres.",
                "action": "Continuar monitoreo de rutina.",
            },
            "Vigilancia": {
                "level": 1,
                "min_risk": 25.0,
                "max_risk": 49.0,
                "exit_threshold": 20.0,
                "legacy": "AMARILLO",
                "color": "#f1c40f",
                "description": "Senales tempranas de deterioro hidrico con necesidad de seguimiento cercano.",
                "action": "Revisar lote, disponibilidad de agua y evolucion del pronostico.",
            },
            "Alerta": {
                "level": 2,
                "min_risk": 50.0,
                "max_risk": 74.0,
                "exit_threshold": 45.0,
                "legacy": "NARANJA",
                "color": "#e67e22",
                "description": "Estres hidrico confirmado con deterioro sostenido y probabilidad de impacto productivo.",
                "action": "Priorizar manejo, suplementacion y monitoreo operativo diario.",
            },
            "Emergencia": {
                "level": 3,
                "min_risk": 75.0,
                "max_risk": 100.0,
                "exit_threshold": 70.0,
                "legacy": "ROJO",
                "color": "#e74c3c",
                "description": "Condicion critica con alta severidad, persistencia y senales convergentes.",
                "action": "Activar respuesta inmediata y plan de contingencia.",
            },
        },
        "risk_weights": {
            "magnitude": float(settings.risk_weight_magnitude),
            "persistence": float(settings.risk_weight_persistence),
            "anomaly": float(settings.risk_weight_anomaly),
            "weather": float(settings.risk_weight_weather),
            "soil": float(settings.risk_weight_soil),
        },
        "confidence_weights": {
            "freshness": float(settings.confidence_weight_freshness),
            "agreement": float(settings.confidence_weight_agreement),
            "applicability": float(settings.confidence_weight_applicability),
            "calibration": float(settings.confidence_weight_calibration),
            "ground_truth": float(settings.confidence_weight_ground_truth),
        },
        "magnitude": {
            "humidity_reference_pct": 60.0,
            "humidity_scale_pct": 45.0,
            "ndmi_reference": 0.25,
            "ndmi_scale": 0.55,
            "estimated_ndmi_reference": 0.25,
            "estimated_ndmi_scale": 0.55,
            "humidity_weight": 55.0,
            "ndmi_weight": 45.0,
        },
        "persistence": {
            "risk_threshold": 50.0,
            "max_consecutive_events": 10,
            "points_per_event": 10.0,
        },
        "anomaly": {
            "history_window_observations": 10,
            "humidity_scale_pct": 30.0,
            "ndmi_scale": 0.30,
        },
        "weather": {
            "spi_reference_abs": 2.0,
            "spi_weight": 45.0,
            "forecast_weight": 55.0,
            "deficit_multiplier": 18.0,
            "temperature_reference_c": 28.0,
            "temperature_multiplier": 2.5,
            "wind_reference_mps": 5.0,
            "wind_multiplier": 4.5,
            "forecast_dryness_weight": 70.0,
            "forecast_spi_weight": 30.0,
            "relief_bonus_if_precip_exceeds_et0": 25.0,
            "improvement_risk_threshold": 45.0,
            "forecast_days_summary": 3,
        },
        "hysteresis": {
            "raise_consecutive_observations": 2,
            "drop_consecutive_observations": 3,
            "emergency_jump_risk": 85.0,
            "emergency_jump_confidence": 70.0,
        },
        "spatial": {
            "actionable_risk_threshold": 50.0,
            "affected_pct_threshold": 35.0,
            "largest_cluster_pct_threshold": 15.0,
            "stable_cluster_min_size": 3,
            "stable_cluster_min_days": 6,
            "analytic_hex_resolution": int(settings.default_hex_resolution),
            "max_hexes_evaluated": 12,
        },
        "calibration": {
            "window_days": int(settings.calibration_window_days),
            "min_samples": int(settings.calibration_min_samples),
            "fallback_id": "fixed_diaz_2026",
            "fallback_quality_score": 58.0,
            "fixed_points": _fixed_points(),
            "coverage_applicability": {
                "pastura_cultivo": 90.0,
                "forestal": 52.0,
                "humedal": 68.0,
                "suelo_desnudo_urbano": 48.0,
            },
        },
        "confidence": {
            "freshness_penalty_per_day": 12.0,
            "agreement_penalty_per_unit": 180.0,
            "simulated_penalty": 12.0,
        },
    }


DEFAULT_ALERT_RULESET = _build_default_ruleset()

SETTINGS_SCHEMA = {
    "schema_version": "1.0",
    "coverage_classes": COVERAGE_OPTIONS,
    "sections": [
        {
            "key": "states",
            "title": "Estados y thresholds",
            "description": "Bandas de riesgo y umbrales de salida por estado.",
            "fields": [
                {"path": "states.Normal.max_risk", "label": "Normal max", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Techo del estado Normal."},
                {"path": "states.Vigilancia.min_risk", "label": "Vigilancia min", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Piso para entrar en Vigilancia."},
                {"path": "states.Vigilancia.max_risk", "label": "Vigilancia max", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Techo del estado Vigilancia."},
                {"path": "states.Vigilancia.exit_threshold", "label": "Vigilancia salida", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Umbral de salida para bajar desde Vigilancia."},
                {"path": "states.Alerta.min_risk", "label": "Alerta min", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Piso para entrar en Alerta."},
                {"path": "states.Alerta.max_risk", "label": "Alerta max", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Techo del estado Alerta."},
                {"path": "states.Alerta.exit_threshold", "label": "Alerta salida", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Umbral de salida para bajar desde Alerta."},
                {"path": "states.Emergencia.min_risk", "label": "Emergencia min", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Piso para entrar en Emergencia."},
                {"path": "states.Emergencia.exit_threshold", "label": "Emergencia salida", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Umbral de salida para bajar desde Emergencia."},
            ],
        },
        {
            "key": "risk_weights",
            "title": "Score de riesgo",
            "description": "Pesos del risk score compuesto. Deben sumar 100.",
            "fields": [
                {"path": "risk_weights.magnitude", "label": "Magnitud", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de la senal observada S1/S2."},
                {"path": "risk_weights.persistence", "label": "Persistencia", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de la persistencia temporal."},
                {"path": "risk_weights.anomaly", "label": "Anomalia", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso del desvio frente a la historia reciente."},
                {"path": "risk_weights.weather", "label": "Confirmacion meteo", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de SPI y forecast."},
                {"path": "risk_weights.soil", "label": "Suelo", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de la vulnerabilidad edafica."},
            ],
        },
        {
            "key": "confidence_weights",
            "title": "Score de confianza",
            "description": "Pesos del confidence score. Deben sumar 100.",
            "fields": [
                {"path": "confidence_weights.freshness", "label": "Frescura", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de frescura y cobertura valida."},
                {"path": "confidence_weights.agreement", "label": "Acuerdo S1-S2", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso del acuerdo entre estimacion y observacion."},
                {"path": "confidence_weights.applicability", "label": "Aplicabilidad", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de la aplicabilidad segun cobertura."},
                {"path": "confidence_weights.calibration", "label": "Calibracion", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de la calidad del ajuste VV-NDMI."},
                {"path": "confidence_weights.ground_truth", "label": "Campo", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso de la validacion de campo."},
            ],
        },
        {
            "key": "magnitude",
            "title": "Magnitud observada",
            "description": "Referencias y escalas para traducir S1 y NDMI a severidad.",
            "fields": [
                {"path": "magnitude.humidity_reference_pct", "label": "Humedad ref", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Valor de humedad usado como referencia de normalidad."},
                {"path": "magnitude.humidity_scale_pct", "label": "Humedad escala", "type": "number", "min": 1, "max": 100, "step": 0.1, "unit": "pts", "help": "Escala de sequedad de la humedad radar."},
                {"path": "magnitude.ndmi_reference", "label": "NDMI ref", "type": "number", "min": -1, "max": 1, "step": 0.01, "unit": "idx", "help": "Valor de referencia para NDMI observado."},
                {"path": "magnitude.ndmi_scale", "label": "NDMI escala", "type": "number", "min": 0.01, "max": 2, "step": 0.01, "unit": "idx", "help": "Escala de sequedad para NDMI observado."},
                {"path": "magnitude.estimated_ndmi_reference", "label": "NDMI est ref", "type": "number", "min": -1, "max": 1, "step": 0.01, "unit": "idx", "help": "Referencia para NDMI estimado por calibracion."},
                {"path": "magnitude.estimated_ndmi_scale", "label": "NDMI est escala", "type": "number", "min": 0.01, "max": 2, "step": 0.01, "unit": "idx", "help": "Escala para NDMI estimado."},
                {"path": "magnitude.humidity_weight", "label": "Peso humedad", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso interno del componente S1."},
                {"path": "magnitude.ndmi_weight", "label": "Peso NDMI", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso interno del componente NDMI."},
            ],
        },
        {
            "key": "persistence_anomaly",
            "title": "Persistencia y anomalia",
            "description": "Ventanas de calculo temporal y puntos por continuidad.",
            "fields": [
                {"path": "persistence.risk_threshold", "label": "Persistencia threshold", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Riesgo minimo para contar persistencia."},
                {"path": "persistence.max_consecutive_events", "label": "Persistencia max eventos", "type": "number", "min": 1, "max": 60, "step": 1, "unit": "obs", "help": "Maximo de observaciones consecutivas consideradas."},
                {"path": "persistence.points_per_event", "label": "Puntos por evento", "type": "number", "min": 0, "max": 50, "step": 0.1, "unit": "pts", "help": "Puntaje agregado por observacion persistente."},
                {"path": "anomaly.history_window_observations", "label": "Ventana anomalia", "type": "number", "min": 1, "max": 60, "step": 1, "unit": "obs", "help": "Numero de observaciones hacia atras."},
                {"path": "anomaly.humidity_scale_pct", "label": "Escala anomalia humedad", "type": "number", "min": 1, "max": 100, "step": 0.1, "unit": "%", "help": "Escala para desvio de humedad."},
                {"path": "anomaly.ndmi_scale", "label": "Escala anomalia NDMI", "type": "number", "min": 0.01, "max": 2, "step": 0.01, "unit": "idx", "help": "Escala para desvio de NDMI."},
            ],
        },
        {
            "key": "weather",
            "title": "Forecast y confirmacion meteorologica",
            "description": "Pesos y coeficientes del bloque meteorologico.",
            "fields": [
                {"path": "weather.spi_reference_abs", "label": "SPI abs ref", "type": "number", "min": 0.1, "max": 5, "step": 0.1, "unit": "SPI", "help": "Referencia absoluta de SPI para 100 puntos."},
                {"path": "weather.spi_weight", "label": "Peso SPI", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso interno del SPI."},
                {"path": "weather.forecast_weight", "label": "Peso forecast", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso interno del forecast."},
                {"path": "weather.deficit_multiplier", "label": "Multiplicador deficit", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "coef", "help": "Coeficiente para ET0 - lluvia."},
                {"path": "weather.temperature_reference_c", "label": "Temp ref", "type": "number", "min": 0, "max": 60, "step": 0.1, "unit": "C", "help": "Temperatura maxima de referencia."},
                {"path": "weather.temperature_multiplier", "label": "Multiplicador temp", "type": "number", "min": 0, "max": 20, "step": 0.1, "unit": "coef", "help": "Coeficiente de penalizacion por calor."},
                {"path": "weather.wind_reference_mps", "label": "Viento ref", "type": "number", "min": 0, "max": 30, "step": 0.1, "unit": "m/s", "help": "Viento maximo de referencia."},
                {"path": "weather.wind_multiplier", "label": "Multiplicador viento", "type": "number", "min": 0, "max": 20, "step": 0.1, "unit": "coef", "help": "Coeficiente de penalizacion por viento."},
                {"path": "weather.forecast_dryness_weight", "label": "Peso sequedad forecast", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso interno del bloque de sequedad forecast."},
                {"path": "weather.forecast_spi_weight", "label": "Peso SPI forecast", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Peso interno del SPI dentro del forecast."},
                {"path": "weather.relief_bonus_if_precip_exceeds_et0", "label": "Bono alivio lluvia", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "pts", "help": "Descuento si precip supera ET0."},
                {"path": "weather.improvement_risk_threshold", "label": "Threshold mejora", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Riesgo maximo proyectado para considerar mejora."},
                {"path": "weather.forecast_days_summary", "label": "Dias forecast", "type": "number", "min": 1, "max": 7, "step": 1, "unit": "dias", "help": "Dias usados en el resumen de presion forecast."},
            ],
        },
        {
            "key": "hysteresis",
            "title": "Histeresis",
            "description": "Reglas para subir, bajar y saltar a emergencia.",
            "fields": [
                {"path": "hysteresis.raise_consecutive_observations", "label": "Subida consecutiva", "type": "number", "min": 1, "max": 10, "step": 1, "unit": "obs", "help": "Observaciones requeridas para subir."},
                {"path": "hysteresis.drop_consecutive_observations", "label": "Bajada consecutiva", "type": "number", "min": 1, "max": 10, "step": 1, "unit": "obs", "help": "Observaciones requeridas para bajar."},
                {"path": "hysteresis.emergency_jump_risk", "label": "Salto emergencia riesgo", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Riesgo minimo para salto directo."},
                {"path": "hysteresis.emergency_jump_confidence", "label": "Salto emergencia confianza", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "conf", "help": "Confianza minima para salto directo."},
            ],
        },
        {
            "key": "spatial",
            "title": "Reglas espaciales",
            "description": "Parametros de accionabilidad por area y cluster.",
            "fields": [
                {"path": "spatial.actionable_risk_threshold", "label": "Risk pixel/hex", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "risk", "help": "Riesgo minimo para contar area afectada."},
                {"path": "spatial.affected_pct_threshold", "label": "% afectado", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Porcentaje minimo de area afectada."},
                {"path": "spatial.largest_cluster_pct_threshold", "label": "% cluster mayor", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "%", "help": "Tamano minimo del cluster principal."},
                {"path": "spatial.stable_cluster_min_size", "label": "Cluster estable tamano", "type": "number", "min": 1, "max": 50, "step": 1, "unit": "hex", "help": "Tamano minimo del cluster estable."},
                {"path": "spatial.stable_cluster_min_days", "label": "Cluster estable dias", "type": "number", "min": 1, "max": 30, "step": 1, "unit": "dias", "help": "Persistencia minima del cluster estable."},
                {"path": "spatial.analytic_hex_resolution", "label": "H3 analitico", "type": "number", "min": 1, "max": 15, "step": 1, "unit": "res", "help": "Resolucion H3 para agregacion analitica."},
                {"path": "spatial.max_hexes_evaluated", "label": "Hex max evaluados", "type": "number", "min": 1, "max": 200, "step": 1, "unit": "hex", "help": "Limite de hexagonos usados en el resumen."},
            ],
        },
        {
            "key": "calibration",
            "title": "Calibracion",
            "description": "Ventanas, muestras minimas y fallback del modelo VV-NDMI.",
            "fields": [
                {"path": "calibration.window_days", "label": "Ventana calibracion", "type": "number", "min": 7, "max": 180, "step": 1, "unit": "dias", "help": "Dias de ventana rodante para recalibracion."},
                {"path": "calibration.min_samples", "label": "Muestras minimas", "type": "number", "min": 3, "max": 500, "step": 1, "unit": "obs", "help": "Observaciones limpias minimas para calibrar."},
                {"path": "calibration.fallback_quality_score", "label": "Calidad fallback", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "score", "help": "Calidad asignada al fallback fijo."},
                {"path": "calibration.fixed_points", "label": "Puntos fijos", "type": "json", "help": "Lista JSON de puntos VV-NDMI para fallback fijo."},
                {"path": "calibration.coverage_applicability", "label": "Aplicabilidad por cobertura", "type": "json", "help": "Mapa JSON de aplicabilidad radar por cobertura."},
            ],
        },
        {
            "key": "confidence",
            "title": "Ajustes de confianza",
            "description": "Penalizaciones aplicadas al confidence score.",
            "fields": [
                {"path": "confidence.freshness_penalty_per_day", "label": "Penalidad frescura/dia", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "pts", "help": "Penalidad diaria por desfasaje temporal."},
                {"path": "confidence.agreement_penalty_per_unit", "label": "Penalidad acuerdo", "type": "number", "min": 0, "max": 1000, "step": 1, "unit": "coef", "help": "Penalidad por diferencia NDMI estimado-observado."},
                {"path": "confidence.simulated_penalty", "label": "Penalidad simulado", "type": "number", "min": 0, "max": 100, "step": 0.1, "unit": "pts", "help": "Descuento de confianza para modo simulado."},
            ],
        },
    ],
}


def _reset_cache() -> None:
    global _SETTINGS_CACHE
    _SETTINGS_CACHE = None


def _set_last_recalculation_status(status: dict[str, Any]) -> None:
    global _LAST_RECALCULATION_STATUS
    _LAST_RECALCULATION_STATUS = {
        "window_days": RECENT_RECALCULATION_DAYS,
        **status,
    }


def _deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged: dict[str, Any] = {}
        for key in set(base) | set(override):
            if key in base and key in override:
                merged[key] = _deep_merge(base[key], override[key])
            elif key in override:
                merged[key] = deepcopy(override[key])
            else:
                merged[key] = deepcopy(base[key])
        return merged
    return deepcopy(override)


def _deep_diff(base: Any, candidate: Any) -> Any:
    if isinstance(base, dict) and isinstance(candidate, dict):
        diff: dict[str, Any] = {}
        for key, value in candidate.items():
            if key not in base:
                diff[key] = deepcopy(value)
                continue
            child = _deep_diff(base[key], value)
            if child not in ({}, None):
                diff[key] = child
        return diff
    if base == candidate:
        return {}
    return deepcopy(candidate)


def _versioned_label(global_version: int, coverage_class: str | None, coverage_version: int | None) -> str:
    if coverage_class and coverage_version:
        return f"global-v{global_version}::{coverage_class}-v{coverage_version}"
    return f"global-v{global_version}"


def _coverage_labels() -> dict[str, str]:
    return {item["key"]: item["label"] for item in COVERAGE_OPTIONS}


def _get_path(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _validate_weights(weights: dict[str, Any], *, label: str) -> None:
    total = round(sum(float(value) for value in weights.values()), 3)
    if abs(total - 100.0) > 0.01:
        raise ValueError(f"Los pesos de {label} deben sumar 100 (actual: {total}).")


def _validate_rules(rules: dict[str, Any]) -> dict[str, Any]:
    _validate_weights(rules["risk_weights"], label="riesgo")
    _validate_weights(rules["confidence_weights"], label="confianza")
    _validate_weights(
        {
            "humidity": rules["magnitude"]["humidity_weight"],
            "ndmi": rules["magnitude"]["ndmi_weight"],
        },
        label="magnitud",
    )
    _validate_weights(
        {
            "spi": rules["weather"]["spi_weight"],
            "forecast": rules["weather"]["forecast_weight"],
        },
        label="bloque meteorologico",
    )
    _validate_weights(
        {
            "forecast_dryness": rules["weather"]["forecast_dryness_weight"],
            "forecast_spi": rules["weather"]["forecast_spi_weight"],
        },
        label="forecast interno",
    )

    ordered_states = sorted(rules["states"].items(), key=lambda item: item[1]["level"])
    previous_max = None
    previous_name = None
    for index, (name, definition) in enumerate(ordered_states):
        min_risk = float(definition["min_risk"])
        max_risk = float(definition["max_risk"])
        exit_threshold = float(definition["exit_threshold"])
        if min_risk > max_risk:
            raise ValueError(f"El estado {name} tiene min_risk mayor a max_risk.")
        if index == 0 and min_risk != 0:
            raise ValueError("El primer estado debe comenzar en 0.")
        if previous_max is not None and min_risk <= previous_max:
            raise ValueError(f"El estado {name} se superpone con {previous_name}.")
        if previous_max is not None and exit_threshold > previous_max:
            raise ValueError(f"El exit_threshold de {name} no puede superar el techo del estado anterior.")
        previous_max = max_risk
        previous_name = name
    if previous_max != 100:
        raise ValueError("El ultimo estado debe terminar en 100.")

    numeric_checks = [
        ("persistence.max_consecutive_events", 1),
        ("anomaly.history_window_observations", 1),
        ("weather.forecast_days_summary", 1),
        ("hysteresis.raise_consecutive_observations", 1),
        ("hysteresis.drop_consecutive_observations", 1),
        ("spatial.stable_cluster_min_size", 1),
        ("spatial.stable_cluster_min_days", 1),
        ("spatial.analytic_hex_resolution", 1),
        ("spatial.max_hexes_evaluated", 1),
        ("calibration.window_days", 1),
        ("calibration.min_samples", 1),
    ]
    for path, minimum in numeric_checks:
        value = _get_path(rules, path)
        if value is None or float(value) < minimum:
            raise ValueError(f"El parametro {path} debe ser mayor o igual a {minimum}.")

    fixed_points = rules["calibration"]["fixed_points"]
    if not isinstance(fixed_points, list) or len(fixed_points) < 2:
        raise ValueError("calibration.fixed_points debe tener al menos dos puntos.")
    last_vv = None
    last_ndmi = None
    for point in fixed_points:
        vv = float(point["vv"])
        ndmi = float(point["ndmi"])
        if last_vv is not None and vv <= last_vv:
            raise ValueError("Los puntos fijos de calibracion deben estar ordenados por VV ascendente.")
        if last_ndmi is not None and ndmi <= last_ndmi:
            raise ValueError("Los puntos fijos de calibracion deben estar ordenados por NDMI ascendente.")
        last_vv = vv
        last_ndmi = ndmi

    applicability = rules["calibration"]["coverage_applicability"]
    for key in COVERAGE_CLASSES:
        if key not in applicability:
            raise ValueError(f"Falta coverage_applicability para {key}.")
        value = float(applicability[key])
        if value < 0 or value > 100:
            raise ValueError(f"coverage_applicability[{key}] debe estar entre 0 y 100.")

    return rules


async def _get_profile(session: AsyncSession, scope_type: str, scope_key: str) -> BusinessSettingsProfile | None:
    result = await session.execute(
        select(BusinessSettingsProfile)
        .where(
            BusinessSettingsProfile.scope_type == scope_type,
            BusinessSettingsProfile.scope_key == scope_key,
        )
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_active_business_settings(session: AsyncSession) -> dict[str, Any]:
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is not None:
        return deepcopy(_SETTINGS_CACHE)

    result = await session.execute(select(BusinessSettingsProfile))
    profiles = list(result.scalars().all())
    global_profile = next((row for row in profiles if row.scope_type == "global" and row.scope_key == "global"), None)
    global_rules = _deep_merge(DEFAULT_ALERT_RULESET, (global_profile.rules_json or {}) if global_profile else {})
    _validate_rules(global_rules)

    overrides: dict[str, dict[str, Any]] = {}
    effective_by_coverage: dict[str, dict[str, Any]] = {}
    for coverage_class in COVERAGE_CLASSES:
        profile = next((row for row in profiles if row.scope_type == "coverage" and row.scope_key == coverage_class), None)
        override_rules = deepcopy(profile.rules_json or {}) if profile else {}
        effective_rules = _deep_merge(global_rules, override_rules)
        _validate_rules(effective_rules)
        overrides[coverage_class] = {
            "rules": override_rules,
            "version": profile.version if profile else 0,
            "updated_at": profile.updated_at.isoformat() if profile and profile.updated_at else None,
            "updated_by_label": profile.updated_by_label if profile else None,
        }
        effective_by_coverage[coverage_class] = effective_rules

    payload = {
        "global": global_rules,
        "global_version": global_profile.version if global_profile else 0,
        "global_updated_at": global_profile.updated_at.isoformat() if global_profile and global_profile.updated_at else None,
        "global_updated_by_label": global_profile.updated_by_label if global_profile else None,
        "overrides": overrides,
        "effective_by_coverage": effective_by_coverage,
        "coverage_labels": _coverage_labels(),
    }
    _SETTINGS_CACHE = deepcopy(payload)
    return deepcopy(payload)


async def get_effective_alert_rules(session: AsyncSession, coverage_class: str | None = None) -> dict[str, Any]:
    active = await get_active_business_settings(session)
    global_version = int(active["global_version"])
    if coverage_class and coverage_class not in COVERAGE_CLASSES:
        raise ValueError(f"Cobertura no soportada: {coverage_class}")
    if coverage_class:
        override = active["overrides"].get(coverage_class, {})
        return {
            "coverage_class": coverage_class,
            "override_active": bool(override.get("rules")),
            "rules": deepcopy(active["effective_by_coverage"][coverage_class]),
            "rules_version": _versioned_label(global_version, coverage_class, int(override.get("version") or 0)),
            "global_version": global_version,
            "coverage_version": int(override.get("version") or 0),
        }
    return {
        "coverage_class": None,
        "override_active": False,
        "rules": deepcopy(active["global"]),
        "rules_version": _versioned_label(global_version, None, None),
        "global_version": global_version,
        "coverage_version": 0,
    }


def get_settings_schema() -> dict[str, Any]:
    return {
        "schema": deepcopy(SETTINGS_SCHEMA),
        "defaults": deepcopy(DEFAULT_ALERT_RULESET),
        "coverage_classes": deepcopy(COVERAGE_OPTIONS),
    }


async def get_settings_payload(session: AsyncSession, coverage_class: str | None = None) -> dict[str, Any]:
    active = await get_active_business_settings(session)
    payload = {
        "global": deepcopy(active["global"]),
        "global_version": active["global_version"],
        "global_updated_at": active["global_updated_at"],
        "global_updated_by_label": active["global_updated_by_label"],
        "overrides": deepcopy(active["overrides"]),
        "effective_by_coverage": deepcopy(active["effective_by_coverage"]),
        "coverage_labels": deepcopy(active["coverage_labels"]),
        "coverage_classes": deepcopy(COVERAGE_OPTIONS),
        "recalculation_status": deepcopy(_LAST_RECALCULATION_STATUS),
    }
    if coverage_class:
        if coverage_class not in COVERAGE_CLASSES:
            raise ValueError(f"Cobertura no soportada: {coverage_class}")
        payload["selected_coverage_class"] = coverage_class
        payload["effective"] = deepcopy(active["effective_by_coverage"][coverage_class])
        payload["override_diff"] = deepcopy(active["overrides"][coverage_class]["rules"])
        payload["rules_version"] = _versioned_label(
            int(active["global_version"]),
            coverage_class,
            int(active["overrides"][coverage_class]["version"] or 0),
        )
    else:
        payload["rules_version"] = _versioned_label(int(active["global_version"]), None, None)
    return payload


async def list_settings_audit(session: AsyncSession, limit: int = 50) -> list[dict[str, Any]]:
    result = await session.execute(
        select(BusinessSettingsAudit)
        .order_by(desc(BusinessSettingsAudit.created_at))
        .limit(limit)
    )
    rows = list(result.scalars().all())
    return [
        {
            "id": row.id,
            "scope_type": row.scope_type,
            "scope_key": row.scope_key,
            "action": row.action,
            "version_before": row.version_before,
            "version_after": row.version_after,
            "updated_from": row.updated_from,
            "updated_by_label": row.updated_by_label,
            "request_ip": row.request_ip,
            "user_agent": row.user_agent,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "previous_rules_json": row.previous_rules_json or {},
            "new_rules_json": row.new_rules_json or {},
        }
        for row in rows
    ]


def _observation_row_to_payload(row: SatelliteObservation) -> dict[str, Any]:
    return {
        "department": row.department,
        "coverage_class": row.coverage_class,
        "vegetation_mask": row.vegetation_mask,
        "source_mode": row.source_mode,
        "s1_vv_db_mean": row.s1_vv_db_mean,
        "s1_humidity_mean_pct": row.s1_humidity_mean_pct,
        "s1_pct_area_stressed": row.s1_pct_area_stressed,
        "s2_ndmi_mean": row.s2_ndmi_mean,
        "s2_valid_pct": row.s2_valid_pct,
        "cloud_cover_pct": row.cloud_cover_pct,
        "lag_hours": row.lag_hours,
        "spi_30d": row.spi_30d,
        "spi_categoria": row.spi_categoria,
        "quality_score": row.quality_score,
        "quality_control": deepcopy(row.quality_control or {}),
        "raw_payload": deepcopy(row.raw_payload or {}),
    }


async def recalculate_recent_business_settings(
    session: AsyncSession,
    *,
    coverage_class: str | None = None,
    window_days: int = RECENT_RECALCULATION_DAYS,
) -> dict[str, Any]:
    from app.services.analysis import (
        analyze_unit,
        backfill_department_spatial_cache,
        run_daily_pipeline,
    )
    from app.services.hexagons import materialize_h3_cache
    from app.services.productive_units import materialize_productive_unit_cache
    from app.services.sections import materialize_police_section_cache

    start_date = date.today() - timedelta(days=max(window_days - 1, 0))
    units_query = select(AOIUnit).where(AOIUnit.unit_type == "department", AOIUnit.active.is_(True))
    if coverage_class:
        units_query = units_query.where(AOIUnit.coverage_class == coverage_class)
    units_result = await session.execute(units_query.order_by(AOIUnit.department))
    units = list(units_result.scalars().all())
    if not units:
        status = {
            "status": "completed",
            "updated_at": _now_utc().isoformat(),
            "processed_units": 0,
            "processed_days": 0,
            "coverage_class": coverage_class,
        }
        _set_last_recalculation_status(status)
        return status

    unit_ids = [unit.id for unit in units]
    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    today_start = datetime.combine(date.today(), time.min, tzinfo=timezone.utc)
    observations_result = await session.execute(
        select(SatelliteObservation)
        .where(
            SatelliteObservation.unit_id.in_(unit_ids),
            SatelliteObservation.observed_at >= start_dt,
            SatelliteObservation.observed_at < today_start,
        )
        .order_by(SatelliteObservation.unit_id, SatelliteObservation.observed_at)
    )
    historical_rows = list(observations_result.scalars().all())
    processed_dates: set[date] = set()
    processed_units: set[str] = set()
    unit_map = {unit.id: unit for unit in units}

    for row in historical_rows:
        unit = unit_map.get(row.unit_id)
        if unit is None:
            continue
        target_date = row.observed_at.date()
        processed_dates.add(target_date)
        processed_units.add(unit.id)
        await analyze_unit(
            session,
            unit=unit,
            target_date=target_date,
            observation_payload=_observation_row_to_payload(row),
            update_current_state=False,
        )

    await run_daily_pipeline(
        session,
        target_date=date.today(),
        update_current_state=True,
        materialize_latest=False,
        refresh_catalog_geometries=False,
    )
    department_result = await backfill_department_spatial_cache(session)
    section_result = await materialize_police_section_cache(
        session,
        target_date=date.today(),
        ensure_base_analysis=False,
        persist_latest=True,
    )
    hex_result = await materialize_h3_cache(
        session,
        target_date=date.today(),
        ensure_base_analysis=False,
        persist_latest=True,
    )
    productive_result = await materialize_productive_unit_cache(
        session,
        target_date=date.today(),
        ensure_base_analysis=False,
        persist_latest=True,
    )
    await session.commit()

    status = {
        "status": "completed",
        "updated_at": _now_utc().isoformat(),
        "processed_units": len(processed_units or unit_ids),
        "processed_days": len(processed_dates),
        "coverage_class": coverage_class,
        "materialized": {
            "departments": department_result.get("count", 0),
            "sections": section_result.get("count", 0),
            "hexagons": hex_result.get("count", 0),
            "productives": productive_result.get("count", 0),
        },
    }
    _set_last_recalculation_status(status)
    return status


async def _write_audit(
    session: AsyncSession,
    *,
    scope_type: str,
    scope_key: str,
    action: str,
    version_before: int | None,
    version_after: int | None,
    previous_rules_json: dict[str, Any] | None,
    new_rules_json: dict[str, Any] | None,
    updated_from: str | None,
    operator_label: str | None,
    request_ip: str | None,
    user_agent: str | None,
) -> None:
    session.add(
        BusinessSettingsAudit(
            scope_type=scope_type,
            scope_key=scope_key,
            action=action,
            version_before=version_before,
            version_after=version_after,
            previous_rules_json=previous_rules_json or {},
            new_rules_json=new_rules_json or {},
            updated_from=updated_from,
            updated_by_label=operator_label,
            request_ip=request_ip,
            user_agent=user_agent,
        )
    )


async def save_global_settings(
    session: AsyncSession,
    rules: dict[str, Any],
    *,
    updated_from: str | None = "settings_ui",
    operator_label: str | None = None,
    request_ip: str | None = None,
    user_agent: str | None = None,
) -> dict[str, Any]:
    current = await _get_profile(session, "global", "global")
    current_rules = _deep_merge(DEFAULT_ALERT_RULESET, (current.rules_json or {}) if current else {})
    candidate = _deep_merge(current_rules, rules or {})
    candidate = _validate_rules(candidate)

    next_version = (current.version if current else 0) + 1
    if current is None:
        current = BusinessSettingsProfile(scope_type="global", scope_key="global")
        session.add(current)
    previous_rules = deepcopy(current.rules_json or current_rules)
    current.rules_json = candidate
    current.version = next_version
    current.updated_from = updated_from
    current.updated_by_label = operator_label
    current.metadata_extra = {"kind": "global"}
    await _write_audit(
        session,
        scope_type="global",
        scope_key="global",
        action="update",
        version_before=(next_version - 1) if next_version > 1 else None,
        version_after=next_version,
        previous_rules_json=previous_rules,
        new_rules_json=candidate,
        updated_from=updated_from,
        operator_label=operator_label,
        request_ip=request_ip,
        user_agent=user_agent,
    )
    await session.commit()
    _reset_cache()
    recalculation_status = await recalculate_recent_business_settings(session)
    payload = await get_settings_payload(session)
    return {
        "status": "success",
        "scope_type": "global",
        "scope_key": "global",
        "rules_version": payload["rules_version"],
        "recalculation_status": recalculation_status,
        "settings": payload,
    }


async def save_coverage_override(
    session: AsyncSession,
    coverage_class: str,
    rules: dict[str, Any],
    *,
    updated_from: str | None = "settings_ui",
    operator_label: str | None = None,
    request_ip: str | None = None,
    user_agent: str | None = None,
) -> dict[str, Any]:
    if coverage_class not in COVERAGE_CLASSES:
        raise ValueError(f"Cobertura no soportada: {coverage_class}")
    active = await get_active_business_settings(session)
    base_rules = deepcopy(active["global"])
    effective_candidate = _deep_merge(base_rules, rules or {})
    effective_candidate = _validate_rules(effective_candidate)
    override_rules = _deep_diff(base_rules, effective_candidate)

    current = await _get_profile(session, "coverage", coverage_class)
    next_version = (current.version if current else 0) + 1
    if current is None:
        current = BusinessSettingsProfile(scope_type="coverage", scope_key=coverage_class)
        session.add(current)
    previous_rules = deepcopy(current.rules_json or {})
    current.rules_json = override_rules
    current.version = next_version
    current.updated_from = updated_from
    current.updated_by_label = operator_label
    current.metadata_extra = {"kind": "coverage_override", "coverage_class": coverage_class}
    await _write_audit(
        session,
        scope_type="coverage",
        scope_key=coverage_class,
        action="update",
        version_before=(next_version - 1) if next_version > 1 else None,
        version_after=next_version,
        previous_rules_json=previous_rules,
        new_rules_json=override_rules,
        updated_from=updated_from,
        operator_label=operator_label,
        request_ip=request_ip,
        user_agent=user_agent,
    )
    await session.commit()
    _reset_cache()
    recalculation_status = await recalculate_recent_business_settings(session, coverage_class=coverage_class)
    payload = await get_settings_payload(session, coverage_class=coverage_class)
    return {
        "status": "success",
        "scope_type": "coverage",
        "scope_key": coverage_class,
        "rules_version": payload["rules_version"],
        "recalculation_status": recalculation_status,
        "settings": payload,
    }


async def reset_global_settings(
    session: AsyncSession,
    *,
    updated_from: str | None = "settings_ui",
    operator_label: str | None = None,
    request_ip: str | None = None,
    user_agent: str | None = None,
) -> dict[str, Any]:
    current = await _get_profile(session, "global", "global")
    previous_rules = deepcopy(current.rules_json or DEFAULT_ALERT_RULESET) if current else deepcopy(DEFAULT_ALERT_RULESET)
    version_before = current.version if current else None
    if current is None:
        current = BusinessSettingsProfile(scope_type="global", scope_key="global")
        session.add(current)
        current.version = 1
    else:
        current.version += 1
    current.rules_json = deepcopy(DEFAULT_ALERT_RULESET)
    current.updated_from = updated_from
    current.updated_by_label = operator_label
    current.metadata_extra = {"kind": "global", "reset": True}
    await _write_audit(
        session,
        scope_type="global",
        scope_key="global",
        action="reset",
        version_before=version_before,
        version_after=current.version,
        previous_rules_json=previous_rules,
        new_rules_json=deepcopy(DEFAULT_ALERT_RULESET),
        updated_from=updated_from,
        operator_label=operator_label,
        request_ip=request_ip,
        user_agent=user_agent,
    )
    await session.commit()
    _reset_cache()
    recalculation_status = await recalculate_recent_business_settings(session)
    payload = await get_settings_payload(session)
    return {
        "status": "success",
        "scope_type": "global",
        "scope_key": "global",
        "rules_version": payload["rules_version"],
        "recalculation_status": recalculation_status,
        "settings": payload,
    }


async def clear_coverage_override(
    session: AsyncSession,
    coverage_class: str,
    *,
    updated_from: str | None = "settings_ui",
    operator_label: str | None = None,
    request_ip: str | None = None,
    user_agent: str | None = None,
) -> dict[str, Any]:
    if coverage_class not in COVERAGE_CLASSES:
        raise ValueError(f"Cobertura no soportada: {coverage_class}")
    current = await _get_profile(session, "coverage", coverage_class)
    if current is None:
        payload = await get_settings_payload(session, coverage_class=coverage_class)
        return {
            "status": "success",
            "scope_type": "coverage",
            "scope_key": coverage_class,
            "rules_version": payload["rules_version"],
            "recalculation_status": deepcopy(_LAST_RECALCULATION_STATUS),
            "settings": payload,
        }

    previous_rules = deepcopy(current.rules_json or {})
    version_before = current.version
    await session.delete(current)
    await _write_audit(
        session,
        scope_type="coverage",
        scope_key=coverage_class,
        action="clear_override",
        version_before=version_before,
        version_after=None,
        previous_rules_json=previous_rules,
        new_rules_json={},
        updated_from=updated_from,
        operator_label=operator_label,
        request_ip=request_ip,
        user_agent=user_agent,
    )
    await session.commit()
    _reset_cache()
    recalculation_status = await recalculate_recent_business_settings(session, coverage_class=coverage_class)
    payload = await get_settings_payload(session, coverage_class=coverage_class)
    return {
        "status": "success",
        "scope_type": "coverage",
        "scope_key": coverage_class,
        "rules_version": payload["rules_version"],
        "recalculation_status": recalculation_status,
        "settings": payload,
    }
