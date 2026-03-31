from __future__ import annotations

from datetime import datetime, timezone
import base64
from email.message import EmailMessage
from email.utils import make_msgid
import logging
from typing import Any

import aiosmtplib
import httpx
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import (
    AlertState,
    AlertSubscription,
    AlertaEvento,
    NotificationEvent,
    SuscriptorAlerta,
)
from app.models.auth import AppUser, AppUserProfile
from app.models.humedad import AOIUnit
from app.services.notification_media import (
    build_national_geometry,
    create_notification_media_assets,
    load_media_asset_bytes,
)


CONFIDENCE_DELTA_THRESHOLD = 15.0
FORECAST_DELTA_THRESHOLD = 10.0
FORECAST_RISK_FLOOR = 55.0
STATE_LEVELS = {"Normal": 0, "Vigilancia": 1, "Alerta": 2, "Emergencia": 3}
SUPPORTED_SUBSCRIPTION_SCOPES = {"productive_unit", "department", "national"}
SUPPORTED_CHANNELS = {"email", "whatsapp"}
logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _forecast_peak(forecast: list[dict[str, Any]] | None) -> float:
    if not forecast:
        return 0.0
    return max(float(item.get("expected_risk") or 0.0) for item in forecast)


def _state_level(value: str | int | None) -> int:
    if isinstance(value, int):
        return value
    return STATE_LEVELS.get(str(value or "Normal"), 0)


def _notification_reasons(current_state: AlertState, previous_state: dict[str, Any] | None = None) -> list[str]:
    reasons: list[str] = []
    current_level = int(current_state.state_level or 0)
    previous_level = int((previous_state or {}).get("state_level") or 0)
    previous_name = (previous_state or {}).get("current_state")

    if previous_state is None:
        if current_level >= 1:
            reasons.append("state_change")
        return reasons

    if previous_name != current_state.current_state:
        reasons.append("state_change")

    previous_confidence = float((previous_state or {}).get("confidence_score") or current_state.confidence_score or 0.0)
    current_confidence = float(current_state.confidence_score or 0.0)
    if abs(current_confidence - previous_confidence) >= CONFIDENCE_DELTA_THRESHOLD:
        reasons.append("confidence_shift")

    previous_peak = _forecast_peak((previous_state or {}).get("forecast") or [])
    current_peak = _forecast_peak(current_state.forecast or [])
    if current_peak >= FORECAST_RISK_FLOOR and (current_peak - previous_peak) >= FORECAST_DELTA_THRESHOLD:
        reasons.append("forecast_deterioration")

    if not reasons and previous_level >= 2 and current_level < previous_level:
        reasons.append("state_change")
    return reasons


def _serialize_subscriber(row: SuscriptorAlerta) -> dict[str, Any]:
    return {
        "id": row.id,
        "nombre": row.nombre,
        "email": row.email,
        "telefono": row.telefono,
        "whatsapp": row.whatsapp,
        "departamento": row.departamento,
        "unit_id": row.unit_id,
        "nivel_minimo": row.nivel_minimo,
        "activo": row.activo,
        "metadata_extra": row.metadata_extra or {},
        "creado_en": row.creado_en.isoformat() if row.creado_en else None,
    }


def _serialize_notification(row: NotificationEvent, event: AlertaEvento | None = None) -> dict[str, Any]:
    payload = row.payload or {}
    return {
        "id": row.id,
        "alert_event_id": row.alert_event_id,
        "channel": row.channel,
        "recipient": row.recipient,
        "status": row.status,
        "reason": row.reason,
        "title": payload.get("title"),
        "body": payload.get("body"),
        "media_assets": payload.get("media_assets", []),
        "provider_response": row.provider_response or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "delivered_at": row.delivered_at.isoformat() if row.delivered_at else None,
        "unit_id": event.unit_id if event else payload.get("unit_id"),
        "department": event.departamento if event else payload.get("department"),
        "state": event.nivel_nombre if event else payload.get("state"),
    }


def _serialize_alert_subscription(row: AlertSubscription) -> dict[str, Any]:
    return {
        "id": row.id,
        "scope_type": row.scope_type,
        "scope_id": row.scope_id,
        "scope_label": row.scope_label,
        "channels_json": list(row.channels_json or []),
        "min_alert_state": row.min_alert_state,
        "active": bool(row.active),
        "last_sent_state": row.last_sent_state,
        "last_sent_at": row.last_sent_at.isoformat() if row.last_sent_at else None,
        "metadata_extra": row.metadata_extra or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _subscriber_channels(subscriber: SuscriptorAlerta, unit_id: str) -> tuple[list[str], dict[str, str]]:
    channels = ["dashboard"]
    recipients = {"dashboard": f"dashboard:{subscriber.id}:{unit_id}"}
    if subscriber.email:
        channels.append("email")
        recipients["email"] = subscriber.email
    if subscriber.telefono:
        channels.append("sms")
        recipients["sms"] = subscriber.telefono
    if subscriber.whatsapp:
        channels.append("whatsapp")
        recipients["whatsapp"] = subscriber.whatsapp
    return channels, recipients


def _trigger_label(reason_key: str) -> str:
    labels = {
        "state_change": "cambio de estado",
        "confidence_shift": "cambio fuerte de confianza",
        "forecast_deterioration": "empeoramiento del pronostico",
        "manual_test": "prueba manual",
    }
    return ", ".join(labels.get(item, item) for item in reason_key.split(",") if item)


def _scope_display(scope_type: str) -> str:
    labels = {
        "productive_unit": "Predio",
        "department": "Departamento",
        "national": "Pais",
    }
    return labels.get(scope_type, scope_type)


def _compose_scope_payload(
    *,
    scope_type: str,
    scope_id: str | None,
    scope_label: str,
    department: str,
    state_name: str,
    state_level: int,
    risk_score: float,
    confidence_score: float,
    affected_pct: float,
    days_in_state: int,
    driver: str,
    forecast: list[dict[str, Any]] | None,
    data_mode: str | None,
    reason_key: str,
    alert_event_id: str | None,
    media_assets: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    forecast_peak = round(_forecast_peak(forecast or []), 1)
    title = f"AgroClimaX | {state_name} | {scope_label}"
    body = (
        f"{_scope_display(scope_type)} {scope_label} ({department}) entra en {state_name} "
        f"por {_trigger_label(reason_key)}. "
        f"Persistencia: {days_in_state} dias. "
        f"Area afectada: {round(affected_pct or 0.0, 1)}%. "
        f"Driver principal: {driver}. "
        f"Forecast max 7d: {forecast_peak}. "
        f"Confianza: {round(confidence_score or 0.0, 1)}%. "
        f"Fuente: {data_mode or 'N/D'}."
    )
    return {
        "title": title,
        "body": body,
        "scope_type": scope_type,
        "scope_id": scope_id,
        "scope_label": scope_label,
        "department": department,
        "state": state_name,
        "state_level": state_level,
        "risk_score": round(risk_score or 0.0, 1),
        "confidence_score": round(confidence_score or 0.0, 1),
        "affected_pct": round(affected_pct or 0.0, 1),
        "days_in_state": days_in_state,
        "forecast_peak_risk": forecast_peak,
        "driver": driver,
        "alert_event_id": alert_event_id,
        "reason_key": reason_key,
        "media_assets": media_assets or [],
    }


def _subscription_dispatch_key(
    subscription: AlertSubscription,
    *,
    alert_event_id: str | None,
    state_name: str,
    reason_key: str,
    observed_at: datetime | None,
) -> str:
    observed_key = observed_at.date().isoformat() if observed_at else "na"
    return f"{subscription.scope_type}:{subscription.scope_id or 'national'}:{alert_event_id or observed_key}:{state_name}:{reason_key}"


def _subscription_recipients(
    user: AppUser,
    profile: AppUserProfile | None,
    subscription: AlertSubscription,
) -> tuple[list[str], dict[str, str]]:
    recipients: dict[str, str] = {}
    channels: list[str] = []
    configured_channels = [channel for channel in subscription.channels_json or [] if channel in SUPPORTED_CHANNELS]
    for channel in configured_channels:
        if channel == "email" and user.email:
            channels.append(channel)
            recipients[channel] = user.email
        elif channel == "whatsapp" and profile and profile.whatsapp_e164:
            channels.append(channel)
            recipients[channel] = profile.whatsapp_e164
    return channels, recipients


def _summarize_dispatch_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "sent": 0,
        "stored": 0,
        "failed": 0,
        "skipped": 0,
        "other": 0,
    }
    for item in results:
        status = str(item.get("status") or "").lower()
        if status in summary:
            summary[status] += 1
        else:
            summary["other"] += 1

    if summary["sent"] or summary["stored"]:
        overall_status = "sent"
    elif summary["failed"]:
        overall_status = "failed"
    elif summary["skipped"]:
        overall_status = "skipped"
    else:
        overall_status = "skipped"

    return {
        "status": overall_status,
        "counts": summary,
        "delivered_channels": [item.get("channel") for item in results if item.get("status") in {"sent", "stored"}],
        "failed_channels": [item.get("channel") for item in results if item.get("status") == "failed"],
        "skipped_channels": [item.get("channel") for item in results if item.get("status") == "skipped"],
    }


class NotificationService:
    async def dispatch(
        self,
        session: AsyncSession,
        *,
        alert_event_id: str | None,
        channels: list[str],
        recipients: dict[str, str],
        payload: dict[str, Any],
        reason: str = "manual_test",
        auto_commit: bool = True,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for channel in channels:
            recipient = recipients.get(channel) or recipients.get("default", "dashboard")
            if channel == "dashboard":
                results.append(
                    await self._record_dashboard_event(
                        session,
                        alert_event_id=alert_event_id,
                        recipient=recipient,
                        payload=payload,
                        reason=reason,
                    )
                )
            elif channel == "email":
                results.append(
                    await self._send_email(
                        session,
                        alert_event_id=alert_event_id,
                        recipient=recipient,
                        payload=payload,
                        reason=reason,
                    )
                )
            elif channel in {"sms", "whatsapp"}:
                results.append(
                    await self._send_twilio_message(
                        session,
                        alert_event_id=alert_event_id,
                        channel=channel,
                        recipient=recipient,
                        payload=payload,
                        reason=reason,
                    )
                )
        if auto_commit:
            await session.commit()
        return results

    async def list_subscribers(
        self,
        session: AsyncSession,
        *,
        unit_id: str | None = None,
        department: str | None = None,
        active_only: bool = False,
    ) -> list[dict[str, Any]]:
        query = select(SuscriptorAlerta).order_by(desc(SuscriptorAlerta.creado_en))
        if unit_id:
            query = query.where((SuscriptorAlerta.unit_id == unit_id) | ((SuscriptorAlerta.unit_id.is_(None)) & (SuscriptorAlerta.departamento == department)))
        elif department:
            query = query.where(SuscriptorAlerta.departamento == department)
        if active_only:
            query = query.where(SuscriptorAlerta.activo.is_(True))
        result = await session.execute(query)
        return [_serialize_subscriber(row) for row in result.scalars().all()]

    async def upsert_subscriber(
        self,
        session: AsyncSession,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        subscriber_id = payload.get("id")
        row = await session.get(SuscriptorAlerta, subscriber_id) if subscriber_id else None
        if row is None:
            row = SuscriptorAlerta()
            session.add(row)

        row.nombre = payload["nombre"]
        row.email = payload.get("email")
        row.telefono = payload.get("telefono")
        row.whatsapp = payload.get("whatsapp")
        row.departamento = payload.get("departamento") or "Rivera"
        row.unit_id = payload.get("unit_id")
        row.nivel_minimo = int(payload.get("nivel_minimo") or 2)
        row.activo = bool(payload.get("activo", True))
        row.metadata_extra = payload.get("metadata_extra") or {}
        await session.commit()
        await session.refresh(row)
        return _serialize_subscriber(row)

    async def delete_subscriber(self, session: AsyncSession, subscriber_id: str) -> dict[str, Any]:
        row = await session.get(SuscriptorAlerta, subscriber_id)
        if row is None:
            raise ValueError("Suscriptor no encontrado")
        await session.delete(row)
        await session.commit()
        return {"status": "deleted", "id": subscriber_id}

    async def list_notification_events(
        self,
        session: AsyncSession,
        *,
        unit_id: str | None = None,
        department: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        query = (
            select(NotificationEvent, AlertaEvento)
            .outerjoin(AlertaEvento, NotificationEvent.alert_event_id == AlertaEvento.id)
            .order_by(desc(NotificationEvent.created_at))
            .limit(limit)
        )
        if unit_id:
            query = query.where(AlertaEvento.unit_id == unit_id)
        elif department:
            query = query.where(AlertaEvento.departamento == department)

        result = await session.execute(query)
        return [_serialize_notification(notification_row, alert_row) for notification_row, alert_row in result.all()]

    async def list_alert_subscriptions(self, session: AsyncSession, *, user: AppUser) -> list[dict[str, Any]]:
        result = await session.execute(
            select(AlertSubscription)
            .where(AlertSubscription.user_id == user.id)
            .order_by(desc(AlertSubscription.updated_at), desc(AlertSubscription.created_at))
        )
        return [_serialize_alert_subscription(item) for item in result.scalars().all()]

    async def get_alert_subscription_options(self, session: AsyncSession, *, user: AppUser) -> dict[str, Any]:
        profile_result = await session.execute(select(AppUserProfile).where(AppUserProfile.user_id == user.id))
        profile = profile_result.scalar_one_or_none()
        departments_result = await session.execute(
            select(AOIUnit).where(AOIUnit.unit_type == "department", AOIUnit.active.is_(True)).order_by(AOIUnit.department)
        )
        productive_result = await session.execute(
            select(AOIUnit).where(AOIUnit.unit_type == "productive_unit", AOIUnit.active.is_(True)).order_by(AOIUnit.department, AOIUnit.name)
        )
        return {
            "scope_types": [
                {"value": "productive_unit", "label": "Predio"},
                {"value": "department", "label": "Departamento"},
                {"value": "national", "label": "Pais"},
            ],
            "min_alert_states": [
                {"value": "Vigilancia", "label": "Vigilancia"},
                {"value": "Alerta", "label": "Alerta"},
                {"value": "Emergencia", "label": "Emergencia"},
            ],
            "channels": [
                {"value": "email", "label": "Email", "enabled": bool(user.email), "reason": None if user.email else "La cuenta no tiene email disponible"},
                {"value": "whatsapp", "label": "WhatsApp", "enabled": bool(profile and profile.whatsapp_e164), "reason": None if profile and profile.whatsapp_e164 else "Completa WhatsApp en tu perfil"},
            ],
            "national": {"value": "national", "label": "Uruguay"},
            "departments": [
                {"id": unit.id, "label": unit.department, "department": unit.department}
                for unit in departments_result.scalars().all()
            ],
            "productive_units": [
                {
                    "id": unit.id,
                    "label": unit.name,
                    "department": unit.department,
                    "unit_category": (unit.metadata_extra or {}).get("unit_category", "predio"),
                }
                for unit in productive_result.scalars().all()
            ],
            "contact": {
                "email": user.email,
                "whatsapp_e164": profile.whatsapp_e164 if profile else None,
            },
        }

    async def save_alert_subscription(
        self,
        session: AsyncSession,
        *,
        user: AppUser,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        scope_type = str(payload.get("scope_type") or "").strip()
        if scope_type not in SUPPORTED_SUBSCRIPTION_SCOPES:
            raise ValueError("scope_type invalido")

        subscription_id = payload.get("id")
        existing = await session.get(AlertSubscription, subscription_id) if subscription_id else None
        if existing is not None and existing.user_id != user.id:
            raise ValueError("Suscripcion no encontrada")

        profile_result = await session.execute(select(AppUserProfile).where(AppUserProfile.user_id == user.id))
        profile = profile_result.scalar_one_or_none()
        channels = [channel for channel in payload.get("channels_json") or [] if channel in SUPPORTED_CHANNELS]
        if not channels:
            raise ValueError("Selecciona al menos un canal")
        if "email" in channels and not user.email:
            raise ValueError("La cuenta autenticada no tiene email disponible")
        if "whatsapp" in channels and not (profile and profile.whatsapp_e164):
            raise ValueError("Completa un numero de WhatsApp en tu perfil antes de habilitar este canal")

        if scope_type == "national":
            scope_id = None
            scope_label = "Uruguay"
        else:
            scope_id = str(payload.get("scope_id") or "").strip()
            if not scope_id:
                raise ValueError("Debes seleccionar un alcance")
            unit_result = await session.execute(select(AOIUnit).where(AOIUnit.id == scope_id, AOIUnit.active.is_(True)))
            unit = unit_result.scalar_one_or_none()
            if unit is None:
                raise ValueError("Unidad no encontrada")
            if scope_type == "productive_unit" and unit.unit_type != "productive_unit":
                raise ValueError("La unidad seleccionada no es un predio/productiva")
            if scope_type == "department" and unit.unit_type != "department":
                raise ValueError("La unidad seleccionada no es un departamento")
            scope_label = unit.name if scope_type == "productive_unit" else unit.department

        row = existing
        if row is None:
            duplicate_result = await session.execute(
                select(AlertSubscription).where(
                    AlertSubscription.user_id == user.id,
                    AlertSubscription.scope_type == scope_type,
                    AlertSubscription.scope_id == scope_id,
                )
            )
            row = duplicate_result.scalar_one_or_none()
        if row is None:
            row = AlertSubscription(user_id=user.id)
            session.add(row)

        row.scope_type = scope_type
        row.scope_id = scope_id
        row.scope_label = scope_label
        row.channels_json = channels
        row.min_alert_state = str(payload.get("min_alert_state") or "Alerta")
        row.active = bool(payload.get("active", True))
        row.metadata_extra = row.metadata_extra or {}
        await session.commit()
        await session.refresh(row)
        return _serialize_alert_subscription(row)

    async def delete_alert_subscription(
        self,
        session: AsyncSession,
        *,
        user: AppUser,
        subscription_id: str,
    ) -> dict[str, Any]:
        row = await session.get(AlertSubscription, subscription_id)
        if row is None or row.user_id != user.id:
            raise ValueError("Suscripcion no encontrada")
        await session.delete(row)
        await session.commit()
        return {"status": "deleted", "id": subscription_id}

    async def send_alert_subscription_test(
        self,
        session: AsyncSession,
        *,
        user: AppUser,
        subscription_id: str,
    ) -> dict[str, Any]:
        row = await session.get(AlertSubscription, subscription_id)
        if row is None or row.user_id != user.id:
            raise ValueError("Suscripcion no encontrada")
        if row.scope_type == "national":
            from app.services.analysis import get_scope_snapshot

            snapshot = await get_scope_snapshot(session, scope="nacional")
            return await self.dispatch_national_alert_subscriptions(
                session,
                current_payload=snapshot,
                previous_payload=None,
                subscription_ids=[row.id],
                reason_override="manual_test",
            )

        unit_result = await session.execute(select(AOIUnit).where(AOIUnit.id == row.scope_id))
        unit = unit_result.scalar_one_or_none()
        if unit is None:
            raise ValueError("Unidad de la suscripcion no encontrada")
        state_result = await session.execute(select(AlertState).where(AlertState.unit_id == unit.id))
        current_state = state_result.scalar_one_or_none()
        if current_state is None:
            from app.services.analysis import get_scope_snapshot

            snapshot = await get_scope_snapshot(session, scope="unidad", unit_id=unit.id)
            current_state = AlertState(
                unit_id=unit.id,
                scope=snapshot.get("scope") or unit.scope,
                department=snapshot.get("department") or unit.department,
                observed_at=datetime.fromisoformat(snapshot["observed_at"]) if snapshot.get("observed_at") else _now_utc(),
                current_state=snapshot.get("state") or "Normal",
                state_level=int(snapshot.get("state_level") or 0),
                risk_score=float(snapshot.get("risk_score") or 0.0),
                confidence_score=float(snapshot.get("confidence_score") or 0.0),
                affected_pct=float(snapshot.get("affected_pct") or 0.0),
                days_in_state=int(snapshot.get("days_in_state") or 0),
                data_mode=snapshot.get("data_mode") or "simulated",
                drivers=snapshot.get("drivers") or [],
                forecast=snapshot.get("forecast") or [],
            )
        return await self._dispatch_configurable_scope_subscriptions(
            session,
            scope_type="department" if unit.unit_type == "department" else "productive_unit",
            scope_id=unit.id,
            scope_label=unit.department if unit.unit_type == "department" else unit.name,
            department=unit.department,
            geometry_geojson=unit.geometry_geojson,
            current_state=current_state,
            alert_event=None,
            reason_key="manual_test",
            subscription_ids=[row.id],
        )

    async def dispatch_operational_alerts(
        self,
        session: AsyncSession,
        *,
        unit: AOIUnit,
        alert_event: AlertaEvento,
        current_state: AlertState,
        previous_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        reason_codes = _notification_reasons(current_state, previous_state)
        reason_key = ",".join(reason_codes)

        legacy_results: list[dict[str, Any]] = []
        if unit.unit_type == "productive_unit" and reason_codes:
            comparison_level = max(
                int(current_state.state_level or 0),
                int((previous_state or {}).get("state_level") or 0),
            )
            subscribers_result = await session.execute(
                select(SuscriptorAlerta)
                .where(
                    SuscriptorAlerta.activo.is_(True),
                    SuscriptorAlerta.nivel_minimo <= comparison_level,
                    ((SuscriptorAlerta.unit_id == unit.id) | ((SuscriptorAlerta.unit_id.is_(None)) & (SuscriptorAlerta.departamento == unit.department))),
                )
                .order_by(desc(SuscriptorAlerta.unit_id))
            )
            subscribers = list(subscribers_result.scalars().all())
            if subscribers:
                payload = _compose_scope_payload(
                    scope_type="productive_unit",
                    scope_id=unit.id,
                    scope_label=unit.name,
                    department=unit.department,
                    state_name=current_state.current_state,
                    state_level=int(current_state.state_level or 0),
                    risk_score=float(current_state.risk_score or 0.0),
                    confidence_score=float(current_state.confidence_score or 0.0),
                    affected_pct=float(current_state.affected_pct or 0.0),
                    days_in_state=int(current_state.days_in_state or 0),
                    driver=((current_state.drivers or [{}])[0] or {}).get("name", "sin driver dominante"),
                    forecast=current_state.forecast or [],
                    data_mode=current_state.data_mode,
                    reason_key=reason_key,
                    alert_event_id=alert_event.id,
                    media_assets=[],
                )
                existing_result = await session.execute(
                    select(NotificationEvent.channel, NotificationEvent.recipient)
                    .where(NotificationEvent.alert_event_id == alert_event.id, NotificationEvent.reason == reason_key)
                )
                existing_pairs = {(channel, recipient) for channel, recipient in existing_result.all()}
                for subscriber in subscribers:
                    channels, recipients = _subscriber_channels(subscriber, unit.id)
                    pending_channels = [channel for channel in channels if (channel, recipients[channel]) not in existing_pairs]
                    if not pending_channels:
                        continue
                    legacy_results.extend(
                        await self.dispatch(
                            session,
                            alert_event_id=alert_event.id,
                            channels=pending_channels,
                            recipients=recipients,
                            payload={**payload, "subscriber_id": subscriber.id, "subscriber_name": subscriber.nombre},
                            reason=reason_key,
                            auto_commit=False,
                        )
                    )

        configurable_result = {"status": "skipped", "reason": "no_operational_trigger", "results": []}
        if reason_codes and unit.unit_type in {"productive_unit", "department"}:
            configurable_result = await self._dispatch_configurable_scope_subscriptions(
                session,
                scope_type="department" if unit.unit_type == "department" else "productive_unit",
                scope_id=unit.id,
                scope_label=unit.department if unit.unit_type == "department" else unit.name,
                department=unit.department,
                geometry_geojson=unit.geometry_geojson,
                current_state=current_state,
                alert_event=alert_event,
                reason_key=reason_key,
            )

        results = legacy_results + configurable_result.get("results", [])
        if not results:
            return {"status": "skipped", "reason": reason_key or "no_operational_trigger", "results": []}
        return {
            "status": "sent",
            "reason": reason_key,
            "legacy_results": legacy_results,
            "configurable_results": configurable_result.get("results", []),
            "results": results,
        }

    async def dispatch_national_alert_subscriptions(
        self,
        session: AsyncSession,
        *,
        current_payload: dict[str, Any],
        previous_payload: dict[str, Any] | None = None,
        subscription_ids: list[str] | None = None,
        reason_override: str | None = None,
    ) -> dict[str, Any]:
        observed_at_raw = current_payload.get("observed_at")
        observed_at = datetime.fromisoformat(observed_at_raw) if observed_at_raw else _now_utc()
        current_state = AlertState(
            unit_id="nacional",
            scope="nacional",
            department="Uruguay",
            observed_at=observed_at,
            current_state=current_payload.get("state") or "Normal",
            state_level=int(current_payload.get("state_level") or _state_level(current_payload.get("state"))),
            risk_score=float(current_payload.get("risk_score") or 0.0),
            confidence_score=float(current_payload.get("confidence_score") or 0.0),
            affected_pct=float(current_payload.get("affected_pct") or 0.0),
            days_in_state=int(current_payload.get("days_in_state") or 0),
            data_mode=current_payload.get("data_mode") or "simulated",
            drivers=current_payload.get("drivers") or [],
            forecast=current_payload.get("forecast") or [],
        )
        previous_state = None
        if previous_payload:
            previous_state = {
                "current_state": previous_payload.get("state"),
                "state_level": previous_payload.get("state_level"),
                "confidence_score": previous_payload.get("confidence_score"),
                "forecast": previous_payload.get("forecast") or [],
            }
        reasons = [reason_override] if reason_override else _notification_reasons(current_state, previous_state)
        if not reasons:
            return {"status": "skipped", "reason": "no_operational_trigger", "results": []}
        national_geometry = await build_national_geometry(session)
        return await self._dispatch_configurable_scope_subscriptions(
            session,
            scope_type="national",
            scope_id=None,
            scope_label="Uruguay",
            department="Uruguay",
            geometry_geojson=national_geometry,
            current_state=current_state,
            alert_event=None,
            reason_key=",".join(reasons),
            subscription_ids=subscription_ids,
        )

    async def _dispatch_configurable_scope_subscriptions(
        self,
        session: AsyncSession,
        *,
        scope_type: str,
        scope_id: str | None,
        scope_label: str,
        department: str,
        geometry_geojson: dict[str, Any] | None,
        current_state: AlertState,
        alert_event: AlertaEvento | None,
        reason_key: str,
        subscription_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        query = (
            select(AlertSubscription, AppUser, AppUserProfile)
            .join(AppUser, AppUser.id == AlertSubscription.user_id)
            .outerjoin(AppUserProfile, AppUserProfile.user_id == AppUser.id)
            .where(
                AlertSubscription.active.is_(True),
                AlertSubscription.scope_type == scope_type,
                AlertSubscription.scope_id == scope_id if scope_type != "national" else AlertSubscription.scope_id.is_(None),
                AppUser.is_active.is_(True),
            )
        )
        if subscription_ids:
            query = query.where(AlertSubscription.id.in_(subscription_ids))
        rows = (await session.execute(query)).all()
        if not rows:
            return {"status": "skipped", "reason": "no_subscriptions", "results": []}

        driver = ((current_state.drivers or [{}])[0] or {}).get("name", "sin driver dominante")
        media_assets = await create_notification_media_assets(
            session,
            scope_type=scope_type,
            scope_id=scope_id,
            scope_label=scope_label,
            geometry_geojson=geometry_geojson,
            state_name=current_state.current_state,
            observed_at=current_state.observed_at,
            department=department,
            risk_score=float(current_state.risk_score or 0.0),
            confidence_score=float(current_state.confidence_score or 0.0),
            affected_pct=float(current_state.affected_pct or 0.0),
            alert_event_id=alert_event.id if alert_event else None,
            subscription_id=None,
        )
        payload = _compose_scope_payload(
            scope_type=scope_type,
            scope_id=scope_id,
            scope_label=scope_label,
            department=department,
            state_name=current_state.current_state,
            state_level=int(current_state.state_level or 0),
            risk_score=float(current_state.risk_score or 0.0),
            confidence_score=float(current_state.confidence_score or 0.0),
            affected_pct=float(current_state.affected_pct or 0.0),
            days_in_state=int(current_state.days_in_state or 0),
            driver=driver,
            forecast=current_state.forecast or [],
            data_mode=current_state.data_mode,
            reason_key=reason_key,
            alert_event_id=alert_event.id if alert_event else None,
            media_assets=media_assets,
        )

        dispatched_results: list[dict[str, Any]] = []
        current_level = int(current_state.state_level or 0)
        for subscription, user, profile in rows:
            if current_level < _state_level(subscription.min_alert_state):
                continue
            dispatch_key = _subscription_dispatch_key(
                subscription,
                alert_event_id=alert_event.id if alert_event else None,
                state_name=current_state.current_state,
                reason_key=reason_key,
                observed_at=current_state.observed_at,
            )
            metadata = subscription.metadata_extra or {}
            if metadata.get("last_dispatch_key") == dispatch_key:
                continue
            channels, recipients = _subscription_recipients(user, profile, subscription)
            if not channels:
                continue
            results = await self.dispatch(
                session,
                alert_event_id=alert_event.id if alert_event else None,
                channels=channels,
                recipients=recipients,
                payload={**payload, "subscription_id": subscription.id, "user_id": user.id},
                reason=reason_key,
                auto_commit=False,
            )
            if any(item.get("status") in {"sent", "stored"} for item in results):
                subscription.last_sent_state = current_state.current_state
                subscription.last_sent_at = _now_utc()
                subscription.metadata_extra = {
                    **metadata,
                    "last_dispatch_key": dispatch_key,
                    "last_reason_key": reason_key,
                    "last_alert_event_id": alert_event.id if alert_event else None,
                }
            dispatched_results.extend(results)

        dispatch_summary = _summarize_dispatch_results(dispatched_results)
        return {
            "status": dispatch_summary["status"] if dispatched_results else "skipped",
            "reason": reason_key if dispatched_results else "no_pending_subscriptions",
            "results": dispatched_results,
            "counts": dispatch_summary["counts"],
            "delivered_channels": dispatch_summary["delivered_channels"],
            "failed_channels": dispatch_summary["failed_channels"],
            "skipped_channels": dispatch_summary["skipped_channels"],
        }

    async def _record_dashboard_event(
        self,
        session: AsyncSession,
        *,
        alert_event_id: str | None,
        recipient: str,
        payload: dict[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        event = NotificationEvent(
            alert_event_id=alert_event_id,
            channel="dashboard",
            recipient=recipient,
            status="stored",
            reason=reason,
            payload=payload,
            provider_response={"message": "dashboard event stored"},
            delivered_at=_now_utc(),
        )
        session.add(event)
        await session.flush()
        return {"channel": "dashboard", "status": "stored", "id": event.id}

    async def _send_email(
        self,
        session: AsyncSession,
        *,
        alert_event_id: str | None,
        recipient: str,
        payload: dict[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        if not (settings.smtp_user and settings.smtp_password and recipient):
            event = NotificationEvent(
                alert_event_id=alert_event_id,
                channel="email",
                recipient=recipient or "missing-recipient",
                status="skipped",
                reason=reason,
                payload=payload,
                provider_response={"message": "SMTP not configured"},
            )
            session.add(event)
            await session.flush()
            return {"channel": "email", "status": "skipped", "id": event.id}

        message = EmailMessage()
        message["From"] = settings.alert_from_email
        message["To"] = recipient
        message["Subject"] = payload.get("title", "AgroClimaX alerta")
        message.set_content(payload.get("body", "Sin contenido"))

        html_parts = [f"<p>{payload.get('body', 'Sin contenido')}</p>"]
        html_inline_assets: list[tuple[str, bytes]] = []
        for asset in payload.get("media_assets") or []:
            storage_key = asset.get("storage_key")
            if not storage_key:
                continue
            content, _ = await load_media_asset_bytes(storage_key)
            cid = make_msgid(domain="agroclimax.local")
            html_inline_assets.append((cid, content))
            html_parts.append(
                f"<div style='margin-top:16px'><strong>{asset.get('kind')}</strong><br /><img src='cid:{cid[1:-1]}' style='max-width:100%;height:auto;border-radius:10px' /></div>"
            )
        if html_parts:
            message.add_alternative("<html><body>" + "".join(html_parts) + "</body></html>", subtype="html")
            html_part = message.get_payload()[-1]
            for cid, content in html_inline_assets:
                html_part.add_related(content, maintype="image", subtype="png", cid=cid)

        event = NotificationEvent(
            alert_event_id=alert_event_id,
            channel="email",
            recipient=recipient,
            status="queued",
            reason=reason,
            payload=payload,
        )
        session.add(event)
        await session.flush()

        try:
            await aiosmtplib.send(
                message,
                hostname=settings.smtp_host,
                port=settings.smtp_port,
                start_tls=True,
                username=settings.smtp_user,
                password=settings.smtp_password,
            )
            event.status = "sent"
            event.delivered_at = _now_utc()
            event.provider_response = {"message": "SMTP accepted"}
        except Exception as exc:  # pragma: no cover
            event.status = "failed"
            event.provider_response = {"error": str(exc)}
            logger.warning("Fallo envio email a %s: %s", recipient, exc)

        return {
            "channel": "email",
            "status": event.status,
            "id": event.id,
            "provider_response": event.provider_response,
        }

    async def _send_twilio_message(
        self,
        session: AsyncSession,
        *,
        alert_event_id: str | None,
        channel: str,
        recipient: str,
        payload: dict[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        if not (settings.twilio_enabled and recipient):
            event = NotificationEvent(
                alert_event_id=alert_event_id,
                channel=channel,
                recipient=recipient or "missing-recipient",
                status="skipped",
                reason=reason,
                payload=payload,
                provider_response={"message": "Twilio not configured"},
            )
            session.add(event)
            await session.flush()
            return {"channel": channel, "status": "skipped", "id": event.id}

        from_number = settings.twilio_sms_from if channel == "sms" else settings.twilio_whatsapp_from
        to_number = recipient if channel == "sms" else f"whatsapp:{recipient.replace('whatsapp:', '')}"
        from_value = from_number if channel == "sms" else f"whatsapp:{from_number.replace('whatsapp:', '')}"

        event = NotificationEvent(
            alert_event_id=alert_event_id,
            channel=channel,
            recipient=recipient,
            status="queued",
            reason=reason,
            payload=payload,
        )
        session.add(event)
        await session.flush()

        auth_token = base64.b64encode(
            f"{settings.twilio_account_sid}:{settings.twilio_auth_token}".encode("utf-8")
        ).decode("utf-8")
        url = f"https://api.twilio.com/2010-04-01/Accounts/{settings.twilio_account_sid}/Messages.json"

        media_urls = []
        if channel == "whatsapp":
            media_urls = [asset.get("url") for asset in payload.get("media_assets") or [] if asset.get("url")]

        post_data: list[tuple[str, str]] = [
            ("To", to_number),
            ("From", from_value),
            ("Body", payload.get("body", "AgroClimaX")),
        ]
        post_data.extend(("MediaUrl", media_url) for media_url in media_urls)

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    url,
                    data=post_data,
                    headers={"Authorization": f"Basic {auth_token}"},
                )
            event.provider_response = {"status_code": response.status_code, "body": response.text[:500]}
            if response.is_success:
                event.status = "sent"
                event.delivered_at = _now_utc()
            else:
                event.status = "failed"
                logger.warning(
                    "Twilio devolvio error para %s -> %s: %s",
                    channel,
                    recipient,
                    event.provider_response,
                )
        except Exception as exc:  # pragma: no cover
            event.status = "failed"
            event.provider_response = {"error": str(exc)}
            logger.warning("Fallo envio Twilio %s a %s: %s", channel, recipient, exc)

        return {
            "channel": channel,
            "status": event.status,
            "id": event.id,
            "provider_response": event.provider_response,
        }


notification_service = NotificationService()
