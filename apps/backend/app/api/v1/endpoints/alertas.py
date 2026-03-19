"""
Endpoints API — Alertas hídricas AgroClimaX.
"""
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from app.db.session import get_db
from app.models.alerta import AlertaEvento
from app.alerts.niveles import NivelAlerta, NIVELES

router = APIRouter(prefix="/alertas", tags=["alertas"])


@router.get("/estado-actual")
async def estado_actual(
    departamento: str = Query("Rivera"),
    db: AsyncSession = Depends(get_db),
):
    """Estado hídrico actual del departamento (último evento registrado)."""
    resultado = await db.execute(
        select(AlertaEvento)
        .where(AlertaEvento.departamento == departamento)
        .order_by(desc(AlertaEvento.fecha))
        .limit(1)
    )
    evento = resultado.scalar_one_or_none()

    if not evento:
        return {
            "departamento": departamento,
            "nivel": "VERDE",
            "nivel_codigo": 0,
            "color": "#2ecc71",
            "mensaje": "Sin datos recientes. Pipeline pendiente de ejecución.",
            "fecha": None,
        }

    return {
        "departamento": departamento,
        "nivel": NivelAlerta(evento.nivel).name,
        "nivel_codigo": evento.nivel,
        "color": NIVELES[NivelAlerta(evento.nivel)].color_hex,
        "humedad_media_pct": evento.humedad_media_pct,
        "ndmi_medio": evento.ndmi_medio,
        "spi": evento.spi_valor,
        "spi_categoria": evento.spi_categoria,
        "pct_area_afectada": evento.pct_area_afectada,
        "es_prolongada": evento.es_prolongada,
        "descripcion": evento.descripcion,
        "accion_recomendada": evento.accion_recomendada,
        "fecha": evento.fecha.isoformat() if evento.fecha else None,
    }


@router.get("/historico")
async def historico(
    departamento: str = Query("Rivera"),
    fecha_inicio: Optional[date] = Query(None),
    fecha_fin: Optional[date] = Query(None),
    limit: int = Query(90, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Historial de alertas para visualización de serie temporal."""
    query = (
        select(AlertaEvento)
        .where(AlertaEvento.departamento == departamento)
        .order_by(desc(AlertaEvento.fecha))
        .limit(limit)
    )

    if fecha_inicio:
        query = query.where(AlertaEvento.fecha >= datetime.combine(fecha_inicio, datetime.min.time()))
    if fecha_fin:
        query = query.where(AlertaEvento.fecha <= datetime.combine(fecha_fin, datetime.max.time()))

    resultado = await db.execute(query)
    eventos = resultado.scalars().all()

    return {
        "departamento": departamento,
        "total": len(eventos),
        "datos": [
            {
                "fecha": e.fecha.isoformat(),
                "nivel": NivelAlerta(e.nivel).name,
                "nivel_codigo": e.nivel,
                "humedad_media_pct": e.humedad_media_pct,
                "ndmi_medio": e.ndmi_medio,
                "spi": e.spi_valor,
                "pct_area_afectada": e.pct_area_afectada,
                "es_prolongada": e.es_prolongada,
            }
            for e in eventos
        ],
    }


@router.get("/niveles")
async def get_niveles():
    """Definición de los niveles de alerta y sus umbrales."""
    return {
        nivel.name: {
            "codigo": int(nivel),
            "nombre": defn.nombre,
            "color": defn.color_hex,
            "humedad_min_pct": defn.humedad_min,
            "humedad_max_pct": defn.humedad_max,
            "ndmi_min": defn.ndmi_min,
            "ndmi_max": defn.ndmi_max,
            "descripcion": defn.descripcion,
            "accion": defn.accion,
        }
        for nivel, defn in NIVELES.items()
    }
