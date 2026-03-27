from __future__ import annotations

from datetime import datetime, timezone
import base64
from typing import Any

import aiosmtplib
import httpx
from email.message import EmailMessage
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import AlertState, AlertaEvento, NotificationEvent, SuscriptorAlerta
from app.models.humedad import AOIUnit


CONFIDENCE_DELTA_THRESHOLD = 15.0
FORECAST_DELTA_THRESHOLD = 10.0
FORECAST_RISK_FLOOR = 55.0


def _forecast_peak(forecast: list[dict[str, Any]] | None) -> float:
    if not forecast:
        return 0.0
    return max(float(item.get("expected_risk") or 0.0) for item in forecast)


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
        "provider_response": row.provider_response or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "delivered_at": row.delivered_at.isoformat() if row.delivered_at else None,
        "unit_id": event.unit_id if event else None,
        "department": event.departamento if event else None,
        "state": event.nivel_nombre if event else None,
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
    }
    return ", ".join(labels.get(item, item) for item in reason_key.split(",") if item)


def _compose_operational_payload(
    unit: AOIUnit,
    alert_event: AlertaEvento,
    current_state: AlertState,
    reason_key: str,
) -> dict[str, Any]:
    driver = ((current_state.drivers or [{}])[0] or {}).get("name", "sin driver dominante")
    forecast_peak = round(_forecast_peak(current_state.forecast or []), 1)
    unit_category = (unit.metadata_extra or {}).get("unit_category", "unidad")
    title = f"AgroClimaX | {current_state.current_state} | {unit.name}"
    body = (
        f"{unit_category.title()} {unit.name} ({unit.department}) entra en {current_state.current_state} "
        f"por {_trigger_label(reason_key)}. "
        f"Persistencia: {current_state.days_in_state} dias. "
        f"Area afectada: {round(current_state.affected_pct or 0.0, 1)}%. "
        f"Driver principal: {driver}. "
        f"Forecast max 7d: {forecast_peak}. "
        f"Confianza: {round(current_state.confidence_score or 0.0, 1)}%. "
        f"Fuente: {current_state.data_mode or 'N/D'}."
    )
    return {
        "title": title,
        "body": body,
        "unit_id": unit.id,
        "unit_name": unit.name,
        "unit_category": unit_category,
        "department": unit.department,
        "state": current_state.current_state,
        "risk_score": round(current_state.risk_score or 0.0, 1),
        "confidence_score": round(current_state.confidence_score or 0.0, 1),
        "affected_pct": round(current_state.affected_pct or 0.0, 1),
        "days_in_state": current_state.days_in_state,
        "forecast_peak_risk": forecast_peak,
        "driver": driver,
        "alert_event_id": alert_event.id,
        "reason_key": reason_key,
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

    async def dispatch_operational_alerts(
        self,
        session: AsyncSession,
        *,
        unit: AOIUnit,
        alert_event: AlertaEvento,
        current_state: AlertState,
        previous_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if unit.unit_type != "productive_unit":
            return {"status": "skipped", "reason": "unsupported_unit_type", "results": []}

        reason_codes = _notification_reasons(current_state, previous_state)
        if not reason_codes:
            return {"status": "skipped", "reason": "no_operational_trigger", "results": []}

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
        if not subscribers:
            return {"status": "skipped", "reason": "no_subscribers", "results": []}

        reason_key = ",".join(reason_codes)
        payload = _compose_operational_payload(unit, alert_event, current_state, reason_key)
        existing_result = await session.execute(
            select(NotificationEvent.channel, NotificationEvent.recipient)
            .where(NotificationEvent.alert_event_id == alert_event.id, NotificationEvent.reason == reason_key)
        )
        existing_pairs = {(channel, recipient) for channel, recipient in existing_result.all()}

        dispatched_results: list[dict[str, Any]] = []
        for subscriber in subscribers:
            channels, recipients = _subscriber_channels(subscriber, unit.id)
            pending_channels = [channel for channel in channels if (channel, recipients[channel]) not in existing_pairs]
            if not pending_channels:
                continue
            dispatched_results.extend(
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

        return {
            "status": "sent" if dispatched_results else "skipped",
            "reason": reason_key if dispatched_results else "already_dispatched",
            "subscriber_count": len(subscribers),
            "results": dispatched_results,
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
            delivered_at=datetime.now(timezone.utc),
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
            event.delivered_at = datetime.now(timezone.utc)
            event.provider_response = {"message": "SMTP accepted"}
        except Exception as exc:  # pragma: no cover
            event.status = "failed"
            event.provider_response = {"error": str(exc)}

        return {"channel": "email", "status": event.status, "id": event.id}

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

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    url,
                    data={"To": to_number, "From": from_value, "Body": payload.get("body", "AgroClimaX")},
                    headers={"Authorization": f"Basic {auth_token}"},
                )
            event.provider_response = {"status_code": response.status_code, "body": response.text[:500]}
            if response.is_success:
                event.status = "sent"
                event.delivered_at = datetime.now(timezone.utc)
            else:
                event.status = "failed"
        except Exception as exc:  # pragma: no cover
            event.status = "failed"
            event.provider_response = {"error": str(exc)}

        return {"channel": channel, "status": event.status, "id": event.id}


notification_service = NotificationService()
