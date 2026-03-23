from fastapi import APIRouter, Depends
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.alerta import AlertaEvento
from app.services.notifications import notification_service

router = APIRouter(prefix="/notificaciones", tags=["notificaciones"])


@router.post("/test")
async def notificacion_test(payload: dict, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AlertaEvento).order_by(desc(AlertaEvento.fecha)).limit(1))
    latest = result.scalar_one_or_none()
    channels = payload.get("channels", ["dashboard", "email", "sms", "whatsapp"])
    recipients = payload.get("recipients", {"default": "dashboard"})
    body = payload.get("body", "AgroClimaX: prueba de cadena de alertas con score, forecast y confidence.")
    title = payload.get("title", "AgroClimaX - prueba de notificacion")
    results = await notification_service.dispatch(
        db,
        alert_event_id=latest.id if latest else None,
        channels=channels,
        recipients=recipients,
        payload={"title": title, "body": body},
        reason="api_test",
    )
    return {"total": len(results), "results": results}
