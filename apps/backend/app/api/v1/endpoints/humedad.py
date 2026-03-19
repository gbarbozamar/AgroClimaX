"""
Endpoints API — Datos de humedad y NDMI por zona.
"""
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc

from app.db.session import get_db
from app.models.humedad import HumedadSuelo

router = APIRouter(prefix="/humedad", tags=["humedad"])


@router.get("/resumen")
async def resumen_humedad(
    fecha: Optional[date] = Query(None, description="Fecha específica. Por defecto: último dato."),
    db: AsyncSession = Depends(get_db),
):
    """
    Resumen estadístico de humedad del suelo para Rivera.
    Incluye percentiles, media, % área en cada nivel.
    """
    if fecha is None:
        ultimo = await db.execute(
            select(func.max(HumedadSuelo.fecha))
        )
        fecha_max = ultimo.scalar_one_or_none()
        if fecha_max is None:
            return {"mensaje": "Sin datos disponibles aún."}
        fecha_dt = fecha_max
    else:
        fecha_dt = datetime.combine(fecha, datetime.min.time())

    resultado = await db.execute(
        select(HumedadSuelo).where(
            HumedadSuelo.fecha >= fecha_dt,
            HumedadSuelo.fecha < fecha_dt.replace(hour=23, minute=59),
        )
    )
    registros = resultado.scalars().all()

    if not registros:
        return {"fecha": str(fecha), "mensaje": "Sin datos para esta fecha."}

    humedades = [r.humedad_s1_pct for r in registros if r.humedad_s1_pct is not None]
    ndmis = [r.ndmi_s2 for r in registros if r.ndmi_s2 is not None]

    import numpy as np
    h = np.array(humedades)
    n = np.array(ndmis)

    return {
        "fecha": fecha_dt.date().isoformat(),
        "n_pixeles": len(registros),
        "humedad_s1": {
            "media": round(float(np.mean(h)), 2) if len(h) > 0 else None,
            "mediana": round(float(np.median(h)), 2) if len(h) > 0 else None,
            "p10": round(float(np.percentile(h, 10)), 2) if len(h) > 0 else None,
            "p25": round(float(np.percentile(h, 25)), 2) if len(h) > 0 else None,
            "p75": round(float(np.percentile(h, 75)), 2) if len(h) > 0 else None,
            "pct_critico": round(100 * float(np.sum(h < 15)) / len(h), 1) if len(h) > 0 else None,
        },
        "ndmi_s2": {
            "media": round(float(np.mean(n)), 4) if len(n) > 0 else None,
            "mediana": round(float(np.median(n)), 4) if len(n) > 0 else None,
        },
    }


@router.get("/serie-temporal")
async def serie_temporal(
    dias: int = Query(30, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Serie temporal diaria de humedad media para Rivera (últimos N días)."""
    resultado = await db.execute(
        select(
            func.date_trunc("day", HumedadSuelo.fecha).label("dia"),
            func.avg(HumedadSuelo.humedad_s1_pct).label("humedad_media"),
            func.avg(HumedadSuelo.ndmi_s2).label("ndmi_medio"),
            func.avg(HumedadSuelo.nivel_alerta).label("nivel_medio"),
        )
        .group_by("dia")
        .order_by(desc("dia"))
        .limit(dias)
    )

    filas = resultado.all()

    return {
        "dias": dias,
        "datos": [
            {
                "fecha": str(f.dia.date()),
                "humedad_media_pct": round(float(f.humedad_media), 2) if f.humedad_media else None,
                "ndmi_medio": round(float(f.ndmi_medio), 4) if f.ndmi_medio else None,
                "nivel_medio": round(float(f.nivel_medio), 2) if f.nivel_medio else None,
            }
            for f in filas
        ],
    }
