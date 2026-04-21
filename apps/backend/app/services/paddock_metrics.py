"""Agregados de métricas por potrero sobre una ventana temporal.

Lee FarmPaddock + UnitIndexSnapshot (app.models.materialized) y computa
risk_score, NDMI y días en alerta para el potrero en los últimos N días.

Convención de unit_id para potreros del usuario: ``f"user-paddock-{paddock_id}"``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from app.models.farm import FarmPaddock
from app.models.materialized import UnitIndexSnapshot

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Umbral para clasificar la tendencia de NDMI entre primeros y últimos 3 snapshots.
_NDMI_FLAT_EPS = 0.02

# state_level >= este valor cuenta como "en alerta" para el paddock.
_ALERT_STATE_LEVEL = 2


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _ndmi_trend(snapshots: list[UnitIndexSnapshot]) -> str:
    """snapshots viene en orden desc (más reciente primero).

    Comparamos la media de los 3 más viejos (cola) vs la media de los 3 más
    nuevos (cabeza) y devolvemos rising / falling / flat.
    """
    ndmi_values = [
        getattr(s, "s2_ndmi_mean", None)
        for s in snapshots
        if getattr(s, "s2_ndmi_mean", None) is not None
    ]
    if len(ndmi_values) < 2:
        return "flat"

    # snapshots es desc por observed_at, por lo que ndmi_values también.
    # "Últimos 3" = los más recientes = prefijo; "primeros 3" = los más viejos = sufijo.
    head = ndmi_values[:3]
    tail = ndmi_values[-3:]
    head_mean = sum(head) / len(head)
    tail_mean = sum(tail) / len(tail)
    diff = head_mean - tail_mean
    if abs(diff) < _NDMI_FLAT_EPS:
        return "flat"
    return "rising" if diff > 0 else "falling"


async def get_paddock_metrics(
    db: "AsyncSession",
    paddock_id: str,
    date_range_days: int = 30,
) -> dict[str, Any]:
    """Retorna métricas agregadas de un potrero en una ventana temporal.

    Lee el FarmPaddock por id y los UnitIndexSnapshot asociados al unit_id
    convencional ``user-paddock-{paddock_id}`` dentro de los últimos
    ``date_range_days`` días, y computa agregados de risk_score / NDMI /
    días en alerta.

    Raises:
        ValueError: si el paddock no existe.
    """
    paddock_result = await db.execute(
        select(FarmPaddock).where(FarmPaddock.id == paddock_id)
    )
    paddock = paddock_result.scalar_one_or_none()
    if paddock is None:
        raise ValueError(f"FarmPaddock not found: {paddock_id}")

    unit_id = f"user-paddock-{paddock_id}"
    since = datetime.now(timezone.utc) - timedelta(days=date_range_days)

    snap_result = await db.execute(
        select(UnitIndexSnapshot)
        .where(UnitIndexSnapshot.unit_id == unit_id)
        .where(UnitIndexSnapshot.observed_at >= since)
        .order_by(UnitIndexSnapshot.observed_at.desc())
    )
    snapshots: list[UnitIndexSnapshot] = list(snap_result.scalars().all())

    base = {
        "paddock_id": paddock_id,
        "paddock_name": paddock.name,
        "field_id": paddock.field_id,
        "area_ha": paddock.area_ha,
        "window_days": date_range_days,
        "n_observations": len(snapshots),
    }

    if not snapshots:
        base.update(
            {
                "risk_score_current": None,
                "risk_score_mean_30d": None,
                "risk_score_max_30d": None,
                "ndmi_current": None,
                "ndmi_trend": "flat",
                "days_in_alert": 0,
                "latest_observed_at": None,
            }
        )
        return base

    latest = snapshots[0]
    risk_scores = [
        float(s.risk_score) for s in snapshots if s.risk_score is not None
    ]

    days_in_alert = sum(
        1
        for s in snapshots
        if (s.state_level is not None and s.state_level >= _ALERT_STATE_LEVEL)
    )

    latest_observed_at = (
        latest.observed_at.isoformat() if latest.observed_at is not None else None
    )

    base.update(
        {
            "risk_score_current": (
                float(latest.risk_score) if latest.risk_score is not None else None
            ),
            "risk_score_mean_30d": _mean(risk_scores),
            "risk_score_max_30d": max(risk_scores) if risk_scores else None,
            "ndmi_current": getattr(latest, "s2_ndmi_mean", None),
            "ndmi_trend": _ndmi_trend(snapshots),
            "days_in_alert": days_in_alert,
            "latest_observed_at": latest_observed_at,
        }
    )
    return base
