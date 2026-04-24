"""
crop_prediction — Heuristic crop outlook stub (Fase 5+).

Predicción simple basada en snapshots recientes del campo. NO es ML real:
es un placeholder heurístico para la superficie MCP mientras se desarrolla
el modelo definitivo. Decisiones de confianza son deliberadamente bajas.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from sqlalchemy import select

from app.models.farm import FarmField
from app.models.field_snapshot import FieldImageSnapshot


_LOOKBACK_DAYS_DEFAULT = 14
_MIN_OBSERVATIONS = 3


def _trend(values: list[float]) -> str:
    """Clasifica tendencia de una serie corta (esperado len==3)."""
    if len(values) < 2:
        return "stable"
    # values está ordenado de más reciente a más viejo; invertimos para leer
    # en orden temporal ascendente.
    chrono = list(reversed(values))
    increasing = all(chrono[i] < chrono[i + 1] for i in range(len(chrono) - 1))
    decreasing = all(chrono[i] > chrono[i + 1] for i in range(len(chrono) - 1))
    if increasing:
        return "improving"
    if decreasing:
        return "declining"
    return "stable"


def _risk_tier(avg_risk: float) -> str:
    if avg_risk > 60:
        return "high"
    if avg_risk >= 30:
        return "moderate"
    return "low"


def _yield_change_pct(ndmi_trend: str, risk_tier: str) -> float:
    if ndmi_trend == "improving" and risk_tier == "low":
        return 5.0
    if ndmi_trend == "declining" and risk_tier == "high":
        return -10.0
    return 0.0


async def predict_crop_outlook(
    db, field_id: str, horizon_days: int = 30
) -> dict[str, Any]:
    """Predicción heurística del outlook del campo basada en snapshots recientes.

    Args:
        db: AsyncSession.
        field_id: UUID del FarmField.
        horizon_days: ventana futura de la predicción (metadata solamente).

    Returns:
        dict con status, tendencia NDMI, risk tier, estimación de cambio de
        rendimiento y confianza (baja por ser heurística).

    Raises:
        ValueError: si el field_id no existe.
    """
    field_row = await db.execute(
        select(FarmField).where(FarmField.id == field_id).limit(1)
    )
    field = field_row.scalar_one_or_none()
    if field is None:
        raise ValueError(f"FarmField {field_id!r} no existe")

    cutoff = date.today() - timedelta(days=_LOOKBACK_DAYS_DEFAULT)
    snapshots_row = await db.execute(
        select(FieldImageSnapshot)
        .where(
            FieldImageSnapshot.field_id == field_id,
            FieldImageSnapshot.observed_at >= cutoff,
        )
        .order_by(FieldImageSnapshot.observed_at.desc())
    )
    snapshots = list(snapshots_row.scalars().all())

    caveats = [
        "Modelo heuristico, no ML real.",
        "Requiere >=3 snapshots recientes para ser no-trivial.",
    ]

    n = len(snapshots)
    base: dict[str, Any] = {
        "field_id": field_id,
        "field_name": field.name,
        "horizon_days": horizon_days,
        "model_version": "heuristic-v0.1",
        "n_observations": n,
        "caveats": caveats,
    }

    if n < _MIN_OBSERVATIONS:
        base.update(
            {
                "status": "insufficient_data",
                "ndmi_trend": "stable",
                "risk_tier": "low",
                "yield_change_pct_estimate": 0.0,
                "confidence": 0.1,
            }
        )
        return base

    # Tomar últimos 3 (snapshots ya ordenados desc por observed_at).
    recent = snapshots[:3]
    ndmi_values = [s.s2_ndmi_mean for s in recent if s.s2_ndmi_mean is not None]
    risk_values = [s.risk_score for s in recent if s.risk_score is not None]

    ndmi_trend = _trend(ndmi_values) if len(ndmi_values) >= 2 else "stable"
    avg_risk = sum(risk_values) / len(risk_values) if risk_values else 0.0
    risk_tier = _risk_tier(avg_risk)
    yield_change_pct = _yield_change_pct(ndmi_trend, risk_tier)

    base.update(
        {
            "status": "ok",
            "ndmi_trend": ndmi_trend,
            "risk_tier": risk_tier,
            "yield_change_pct_estimate": yield_change_pct,
            "confidence": 0.4,
        }
    )
    return base
