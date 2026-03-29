from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.auth import AppUser, AppUserProfile
from app.models.humedad import AOIUnit


QUESTIONNAIRE_VERSION = "v1"
E164_PATTERN = re.compile(r"^\+\d{8,20}$")

ORGANIZATION_TYPES = (
    {"value": "productor", "label": "Productor"},
    {"value": "empresa_agro", "label": "Empresa agro"},
    {"value": "consultora", "label": "Consultora"},
    {"value": "gobierno", "label": "Gobierno"},
    {"value": "academia", "label": "Academia"},
    {"value": "ong", "label": "ONG"},
    {"value": "otro", "label": "Otro"},
)

ROLE_CODES = (
    {"value": "productor", "label": "Productor"},
    {"value": "asesor_tecnico", "label": "Asesor tecnico"},
    {"value": "ingeniero_agronomo", "label": "Ingeniero agronomo"},
    {"value": "veterinario", "label": "Veterinario"},
    {"value": "operador", "label": "Operador"},
    {"value": "analista", "label": "Analista"},
    {"value": "investigador", "label": "Investigador"},
    {"value": "administrador_publico", "label": "Administrador publico"},
    {"value": "otro", "label": "Otro"},
)

SCOPE_TYPES = (
    {"value": "nacional", "label": "Nacional"},
    {"value": "departamento", "label": "Departamento"},
    {"value": "jurisdiccion", "label": "Jurisdiccion"},
)

PRODUCTION_TYPES = (
    {"value": "ganaderia", "label": "Ganaderia"},
    {"value": "agricultura", "label": "Agricultura"},
    {"value": "forestal", "label": "Forestal"},
    {"value": "lecheria", "label": "Lecheria"},
    {"value": "mixto", "label": "Mixto"},
    {"value": "otro", "label": "Otro"},
)

USE_CASES = (
    {"value": "monitoreo_diario", "label": "Monitoreo diario"},
    {"value": "gestion_alertas", "label": "Gestion de alertas"},
    {"value": "seguimiento_productivo", "label": "Seguimiento productivo"},
    {"value": "reporting", "label": "Reporting"},
    {"value": "analisis_territorial", "label": "Analisis territorial"},
    {"value": "investigacion", "label": "Investigacion"},
)

ALERT_CHANNELS = (
    {"value": "email", "label": "Email"},
    {"value": "whatsapp", "label": "WhatsApp"},
    {"value": "sms", "label": "SMS"},
)

MIN_ALERT_STATES = (
    {"value": "Vigilancia", "label": "Vigilancia"},
    {"value": "Alerta", "label": "Alerta"},
    {"value": "Emergencia", "label": "Emergencia"},
)

PREFERRED_LANGUAGES = (
    {"value": "es-UY", "label": "Espanol (Uruguay)"},
    {"value": "pt-BR", "label": "Portugues"},
    {"value": "en", "label": "English"},
)

PROFILE_COMPLETION_FIELDS = (
    ("role_code", "Rol"),
    ("organization_type", "Tipo de organizacion"),
    ("scope_type", "Cobertura operativa"),
    ("scope_ids_json", "Ambitos operativos"),
    ("use_cases_json", "Casos de uso"),
    ("alert_channels_json", "Canales de alerta"),
    ("data_usage_consent_at", "Consentimiento de uso de datos"),
    ("phone_if_sms", "Telefono SMS"),
    ("whatsapp_if_enabled", "Telefono WhatsApp"),
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _clean_list(value: list[str] | None) -> list[str]:
    if not value:
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _serialize_google_identity(user: AppUser) -> dict[str, Any]:
    return {
        "id": user.id,
        "google_sub": user.google_sub,
        "email": user.email,
        "email_verified": bool(user.email_verified),
        "full_name": user.full_name,
        "given_name": user.given_name,
        "family_name": user.family_name,
        "picture_url": user.picture_url,
        "locale": user.locale,
        "is_active": bool(user.is_active),
        "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }


def _empty_profile() -> dict[str, Any]:
    return {
        "phone_e164": None,
        "whatsapp_e164": None,
        "organization_name": None,
        "organization_type": None,
        "role_code": None,
        "job_title": None,
        "scope_type": None,
        "scope_ids_json": [],
        "production_type": None,
        "operation_size_hectares": None,
        "livestock_headcount": None,
        "crop_types_json": [],
        "use_cases_json": [],
        "alert_channels_json": [],
        "min_alert_state": "Alerta",
        "preferred_language": "es-UY",
        "communications_opt_in": False,
        "data_usage_consent_at": None,
        "questionnaire_version": QUESTIONNAIRE_VERSION,
        "completion_pct": 0.0,
        "profile_completed_at": None,
        "created_at": None,
        "updated_at": None,
    }


def _serialize_profile(profile: AppUserProfile | None) -> dict[str, Any]:
    if profile is None:
        return _empty_profile()
    return {
        "phone_e164": profile.phone_e164,
        "whatsapp_e164": profile.whatsapp_e164,
        "organization_name": profile.organization_name,
        "organization_type": profile.organization_type,
        "role_code": profile.role_code,
        "job_title": profile.job_title,
        "scope_type": profile.scope_type,
        "scope_ids_json": list(profile.scope_ids_json or []),
        "production_type": profile.production_type,
        "operation_size_hectares": profile.operation_size_hectares,
        "livestock_headcount": profile.livestock_headcount,
        "crop_types_json": list(profile.crop_types_json or []),
        "use_cases_json": list(profile.use_cases_json or []),
        "alert_channels_json": list(profile.alert_channels_json or []),
        "min_alert_state": profile.min_alert_state,
        "preferred_language": profile.preferred_language,
        "communications_opt_in": bool(profile.communications_opt_in),
        "data_usage_consent_at": profile.data_usage_consent_at.isoformat() if profile.data_usage_consent_at else None,
        "questionnaire_version": profile.questionnaire_version or QUESTIONNAIRE_VERSION,
        "completion_pct": round(float(profile.completion_pct or 0.0), 1),
        "profile_completed_at": profile.profile_completed_at.isoformat() if profile.profile_completed_at else None,
        "created_at": profile.created_at.isoformat() if profile.created_at else None,
        "updated_at": profile.updated_at.isoformat() if profile.updated_at else None,
    }


def _coerce_profile_input(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "phone_e164": _clean_text(payload.get("phone_e164")),
        "whatsapp_e164": _clean_text(payload.get("whatsapp_e164")),
        "organization_name": _clean_text(payload.get("organization_name")),
        "organization_type": _clean_text(payload.get("organization_type")),
        "role_code": _clean_text(payload.get("role_code")),
        "job_title": _clean_text(payload.get("job_title")),
        "scope_type": _clean_text(payload.get("scope_type")),
        "scope_ids_json": _clean_list(payload.get("scope_ids_json")),
        "production_type": _clean_text(payload.get("production_type")),
        "operation_size_hectares": payload.get("operation_size_hectares"),
        "livestock_headcount": payload.get("livestock_headcount"),
        "crop_types_json": _clean_list(payload.get("crop_types_json")),
        "use_cases_json": _clean_list(payload.get("use_cases_json")),
        "alert_channels_json": _clean_list(payload.get("alert_channels_json")),
        "min_alert_state": _clean_text(payload.get("min_alert_state")) or "Alerta",
        "preferred_language": _clean_text(payload.get("preferred_language")) or "es-UY",
        "communications_opt_in": bool(payload.get("communications_opt_in")),
        "data_usage_consent": bool(payload.get("data_usage_consent")),
    }


def _validate_phone(field_name: str, value: str | None) -> None:
    if value is None:
        return
    if not E164_PATTERN.match(value):
        raise ValueError(f"{field_name} debe estar en formato E.164, por ejemplo +59899111222")


async def _get_departments(session: AsyncSession) -> list[dict[str, str]]:
    result = await session.execute(
        select(AOIUnit.id, AOIUnit.department)
        .where(AOIUnit.unit_type == "department", AOIUnit.active.is_(True))
        .order_by(AOIUnit.department)
    )
    rows = result.all()
    return [{"id": row.id, "label": row.department} for row in rows]


async def _get_jurisdictions(session: AsyncSession) -> list[dict[str, str]]:
    result = await session.execute(
        select(AOIUnit.id, AOIUnit.name, AOIUnit.department)
        .where(AOIUnit.unit_type == "police_section", AOIUnit.active.is_(True))
        .order_by(AOIUnit.department, AOIUnit.name)
    )
    rows = result.all()
    return [{"id": row.id, "label": row.name, "department": row.department} for row in rows]


async def get_profile_schema(session: AsyncSession) -> dict[str, Any]:
    departments = await _get_departments(session)
    jurisdictions = await _get_jurisdictions(session)
    return {
        "questionnaire_version": QUESTIONNAIRE_VERSION,
        "required_fields": [field for field, _ in PROFILE_COMPLETION_FIELDS],
        "catalogs": {
            "organization_types": list(ORGANIZATION_TYPES),
            "role_codes": list(ROLE_CODES),
            "scope_types": list(SCOPE_TYPES),
            "production_types": list(PRODUCTION_TYPES),
            "use_cases": list(USE_CASES),
            "alert_channels": list(ALERT_CHANNELS),
            "min_alert_states": list(MIN_ALERT_STATES),
            "preferred_languages": list(PREFERRED_LANGUAGES),
            "departments": departments,
            "jurisdictions": jurisdictions,
        },
    }


async def _get_user_profile(session: AsyncSession, user_id: str) -> AppUserProfile | None:
    result = await session.execute(select(AppUserProfile).where(AppUserProfile.user_id == user_id))
    return result.scalar_one_or_none()


def _compute_completion(profile_data: dict[str, Any]) -> tuple[float, list[str]]:
    missing: list[str] = []

    checks = {
        "role_code": bool(profile_data.get("role_code")),
        "organization_type": bool(profile_data.get("organization_type")),
        "scope_type": bool(profile_data.get("scope_type")),
        "scope_ids_json": (
            profile_data.get("scope_type") == "nacional"
            or bool(profile_data.get("scope_ids_json"))
        ),
        "use_cases_json": bool(profile_data.get("use_cases_json")),
        "alert_channels_json": bool(profile_data.get("alert_channels_json")),
        "data_usage_consent_at": bool(profile_data.get("data_usage_consent_at")),
        "phone_if_sms": ("sms" not in profile_data.get("alert_channels_json", [])) or bool(profile_data.get("phone_e164")),
        "whatsapp_if_enabled": ("whatsapp" not in profile_data.get("alert_channels_json", [])) or bool(profile_data.get("whatsapp_e164")),
    }

    for key, label in PROFILE_COMPLETION_FIELDS:
        if not checks.get(key, False):
            missing.append(label)

    completion = (len(PROFILE_COMPLETION_FIELDS) - len(missing)) / len(PROFILE_COMPLETION_FIELDS) * 100.0
    return round(completion, 1), missing


def _completion_payload(profile: AppUserProfile | None, missing_fields: list[str] | None = None) -> dict[str, Any]:
    completion_pct = round(float(profile.completion_pct or 0.0), 1) if profile is not None else 0.0
    if missing_fields is None:
        missing_fields = []
    return {
        "is_complete": completion_pct >= 100.0,
        "completion_pct": completion_pct,
        "questionnaire_version": (profile.questionnaire_version if profile else QUESTIONNAIRE_VERSION),
        "completed_at": profile.profile_completed_at.isoformat() if profile and profile.profile_completed_at else None,
        "missing_fields": missing_fields,
    }


async def get_profile_status(session: AsyncSession, user_id: str) -> dict[str, Any]:
    profile = await _get_user_profile(session, user_id)
    if profile is None:
        return _completion_payload(None, [label for _, label in PROFILE_COMPLETION_FIELDS])
    completion_pct, missing_fields = _compute_completion(_serialize_profile(profile))
    if round(float(profile.completion_pct or 0.0), 1) != completion_pct:
        profile.completion_pct = completion_pct
        await session.commit()
        await session.refresh(profile)
    return _completion_payload(profile, missing_fields)


async def get_profile_me(session: AsyncSession, user: AppUser) -> dict[str, Any]:
    profile = await _get_user_profile(session, user.id)
    schema = await get_profile_schema(session)
    completion = await get_profile_status(session, user.id)
    return {
        "google_identity": _serialize_google_identity(user),
        "profile": _serialize_profile(profile),
        "completion": completion,
        "options": schema["catalogs"],
    }


async def _ensure_user_exists(session: AsyncSession, user: AppUser) -> AppUser:
    result = await session.execute(select(AppUser).where(AppUser.id == user.id))
    existing = result.scalar_one_or_none()
    if existing is not None:
        return existing
    session.add(user)
    await session.flush()
    return user


def _validate_catalog_value(field_name: str, value: str | None, options: tuple[dict[str, str], ...]) -> None:
    if value is None:
        return
    allowed = {item["value"] for item in options}
    if value not in allowed:
        raise ValueError(f"{field_name} no es valido")


async def save_profile_me(session: AsyncSession, user: AppUser, payload: dict[str, Any]) -> dict[str, Any]:
    data = _coerce_profile_input(payload)

    _validate_phone("phone_e164", data["phone_e164"])
    _validate_phone("whatsapp_e164", data["whatsapp_e164"])
    _validate_catalog_value("organization_type", data["organization_type"], ORGANIZATION_TYPES)
    _validate_catalog_value("role_code", data["role_code"], ROLE_CODES)
    _validate_catalog_value("scope_type", data["scope_type"], SCOPE_TYPES)
    _validate_catalog_value("production_type", data["production_type"], PRODUCTION_TYPES)
    _validate_catalog_value("min_alert_state", data["min_alert_state"], MIN_ALERT_STATES)
    _validate_catalog_value("preferred_language", data["preferred_language"], PREFERRED_LANGUAGES)

    allowed_use_cases = {item["value"] for item in USE_CASES}
    if any(item not in allowed_use_cases for item in data["use_cases_json"]):
        raise ValueError("use_cases_json contiene valores no validos")

    allowed_channels = {item["value"] for item in ALERT_CHANNELS}
    if any(item not in allowed_channels for item in data["alert_channels_json"]):
        raise ValueError("alert_channels_json contiene valores no validos")

    if data["scope_type"] == "departamento":
        allowed_scope_ids = {item["id"] for item in await _get_departments(session)}
        if any(item not in allowed_scope_ids for item in data["scope_ids_json"]):
            raise ValueError("scope_ids_json contiene departamentos no validos")
    elif data["scope_type"] == "jurisdiccion":
        allowed_scope_ids = {item["id"] for item in await _get_jurisdictions(session)}
        if any(item not in allowed_scope_ids for item in data["scope_ids_json"]):
            raise ValueError("scope_ids_json contiene jurisdicciones no validas")
    elif data["scope_type"] == "nacional":
        data["scope_ids_json"] = []

    if data["operation_size_hectares"] is not None and float(data["operation_size_hectares"]) < 0:
        raise ValueError("operation_size_hectares no puede ser negativo")
    if data["livestock_headcount"] is not None and float(data["livestock_headcount"]) < 0:
        raise ValueError("livestock_headcount no puede ser negativo")

    await _ensure_user_exists(session, user)
    profile = await _get_user_profile(session, user.id)
    if profile is None:
        profile = AppUserProfile(user_id=user.id)
        session.add(profile)

    profile.phone_e164 = data["phone_e164"]
    profile.whatsapp_e164 = data["whatsapp_e164"]
    profile.organization_name = data["organization_name"]
    profile.organization_type = data["organization_type"]
    profile.role_code = data["role_code"]
    profile.job_title = data["job_title"]
    profile.scope_type = data["scope_type"]
    profile.scope_ids_json = data["scope_ids_json"]
    profile.production_type = data["production_type"]
    profile.operation_size_hectares = float(data["operation_size_hectares"]) if data["operation_size_hectares"] not in (None, "") else None
    profile.livestock_headcount = float(data["livestock_headcount"]) if data["livestock_headcount"] not in (None, "") else None
    profile.crop_types_json = data["crop_types_json"]
    profile.use_cases_json = data["use_cases_json"]
    profile.alert_channels_json = data["alert_channels_json"]
    profile.min_alert_state = data["min_alert_state"]
    profile.preferred_language = data["preferred_language"]
    profile.communications_opt_in = data["communications_opt_in"]
    profile.questionnaire_version = QUESTIONNAIRE_VERSION
    profile.data_usage_consent_at = _now_utc() if data["data_usage_consent"] else None

    serialized = _serialize_profile(profile)
    completion_pct, missing_fields = _compute_completion(serialized)
    profile.completion_pct = completion_pct
    if completion_pct >= 100.0 and profile.profile_completed_at is None:
        profile.profile_completed_at = _now_utc()

    await session.commit()
    await session.refresh(profile)

    schema = await get_profile_schema(session)
    return {
        "google_identity": _serialize_google_identity(user),
        "profile": _serialize_profile(profile),
        "completion": _completion_payload(profile, missing_fields),
        "options": schema["catalogs"],
    }
