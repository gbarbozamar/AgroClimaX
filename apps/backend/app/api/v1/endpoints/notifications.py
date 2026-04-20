from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.alerta import AlertaEvento
from app.services.notifications import notification_service

logger = logging.getLogger(__name__)
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
    try:
        result = await db.execute(select(AlertaEvento).order_by(desc(AlertaEvento.fecha)).limit(1))
        latest = result.scalar_one_or_none()
    except Exception as exc:
        logger.exception("notificacion_test: error buscando ultimo AlertaEvento")
        raise HTTPException(status_code=500, detail=f"DB error resolviendo el ultimo evento: {exc}") from exc

    channels = payload.get("channels", ["dashboard", "email", "sms", "whatsapp"])
    recipients = payload.get("recipients", {"default": "dashboard"})
    body = payload.get("body", "AgroClimaX: prueba de cadena de alertas con score, forecast y confidence.")
    title = payload.get("title", "AgroClimaX - prueba de notificacion")
    try:
        results = await notification_service.dispatch(
            db,
            alert_event_id=latest.id if latest else None,
            channels=channels,
            recipients=recipients,
            payload={"title": title, "body": body},
            reason="api_test",
        )
    except Exception as exc:
        logger.exception("notificacion_test: dispatch fallo")
        raise HTTPException(status_code=502, detail=f"Dispatch fallo: {exc}") from exc
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
    try:
        return await notification_service.upsert_subscriber(db, payload.model_dump())
    except ValueError as exc:
        logger.warning("guardar_suscriptor: payload invalido: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IntegrityError as exc:
        await db.rollback()
        logger.warning("guardar_suscriptor: integrity error: %s", exc)
        raise HTTPException(status_code=409, detail="Conflicto guardando suscriptor (duplicado o FK invalida)") from exc
    except Exception as exc:
        await db.rollback()
        logger.exception("guardar_suscriptor: error inesperado")
        raise HTTPException(status_code=500, detail=f"Error guardando suscriptor: {exc}") from exc


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
