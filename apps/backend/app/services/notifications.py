from __future__ import annotations

from datetime import datetime, timezone
import base64
from typing import Any

import aiosmtplib
import httpx
from email.message import EmailMessage
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import NotificationEvent


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
        await session.commit()
        return results

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
        url = (
            f"https://api.twilio.com/2010-04-01/Accounts/"
            f"{settings.twilio_account_sid}/Messages.json"
        )

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
