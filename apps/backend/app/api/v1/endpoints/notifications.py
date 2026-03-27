from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.alerta import AlertaEvento
from app.services.notifications import notification_service

router = APIRouter(prefix="/notificaciones", tags=["notificaciones"])


class SubscriberRequest(BaseModel):
    id: str | None = None
    nombre: str
    email: str | None = None
    telefono: str | None = None
    whatsapp: str | None = None
    departamento: str = "Rivera"
    unit_id: str | None = None
    nivel_minimo: int = Field(default=2, ge=0, le=3)
    activo: bool = True
    metadata_extra: dict[str, Any] = Field(default_factory=dict)


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


@router.get("/suscriptores")
async def listar_suscriptores(
    unit_id: str | None = Query(None),
    department: str | None = Query(None),
    active_only: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    datos = await notification_service.list_subscribers(
        db,
        unit_id=unit_id,
        department=department,
        active_only=active_only,
    )
    return {"total": len(datos), "datos": datos}


@router.post("/suscriptores")
async def guardar_suscriptor(payload: SubscriberRequest, db: AsyncSession = Depends(get_db)):
    return await notification_service.upsert_subscriber(db, payload.model_dump())


@router.delete("/suscriptores/{subscriber_id}")
async def eliminar_suscriptor(subscriber_id: str, db: AsyncSession = Depends(get_db)):
    return await notification_service.delete_subscriber(db, subscriber_id)


@router.get("/eventos")
async def listar_eventos_notificacion(
    unit_id: str | None = Query(None),
    department: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    datos = await notification_service.list_notification_events(
        db,
        unit_id=unit_id,
        department=department,
        limit=limit,
    )
    return {"total": len(datos), "datos": datos}
