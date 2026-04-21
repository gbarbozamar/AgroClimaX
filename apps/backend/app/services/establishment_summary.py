"""Servicio para computar resumen agregado de un establecimiento.

Agrega, para un `FarmEstablishment`, los `FarmField` activos que le pertenecen,
contando paddocks activos, sumando áreas y adjuntando el `UnitIndexSnapshot`
más reciente por unit_id virtual `user-field-{field.id}`.

Uso típico: endpoints MCP / dashboards que muestran overview por establecimiento
sin tener que re-encadenar queries desde el frontend.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from app.models.farm import FarmEstablishment, FarmField, FarmPaddock
from app.models.materialized import UnitIndexSnapshot

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def get_establishment_summary(
    db: "AsyncSession", establishment_id: str
) -> dict[str, Any]:
    """Resumen agregado de todos los campos y potreros de un establecimiento.

    Parameters
    ----------
    db:
        Sesión AsyncSession de SQLAlchemy.
    establishment_id:
        ID del `FarmEstablishment`.

    Returns
    -------
    dict con metadata del establecimiento, lista de campos (cada uno con conteo
    de paddocks activos, área, risk_score/state del último snapshot) y totales
    agregados.

    Raises
    ------
    ValueError
        Si `establishment_id` no existe.
    """
    # 1. Leer establishment.
    est_result = await db.execute(
        select(FarmEstablishment).where(FarmEstablishment.id == establishment_id)
    )
    est = est_result.scalar_one_or_none()
    if est is None:
        raise ValueError(f"FarmEstablishment not found: {establishment_id}")

    # 2. Fields activos del establishment.
    fields_result = await db.execute(
        select(FarmField)
        .where(FarmField.establishment_id == establishment_id)
        .where(FarmField.active.is_(True))
    )
    fields = list(fields_result.scalars().all())

    # 3. Conteo de paddocks activos por field_id.
    paddock_counts: dict[str, int] = {}
    if fields:
        field_ids = [f.id for f in fields]
        counts_result = await db.execute(
            select(FarmPaddock.field_id, func.count(FarmPaddock.id))
            .where(FarmPaddock.field_id.in_(field_ids))
            .where(FarmPaddock.active.is_(True))
            .group_by(FarmPaddock.field_id)
        )
        for fid, cnt in counts_result.all():
            paddock_counts[fid] = int(cnt)

    # 4. Último UnitIndexSnapshot por unit_id `user-field-{id}`.
    snapshots_by_unit: dict[str, UnitIndexSnapshot] = {}
    if fields:
        unit_ids = [f"user-field-{f.id}" for f in fields]
        # Traer todos los snapshots de esos unit_ids y quedarnos con el más reciente.
        snap_result = await db.execute(
            select(UnitIndexSnapshot)
            .where(UnitIndexSnapshot.unit_id.in_(unit_ids))
            .order_by(UnitIndexSnapshot.observed_at.desc())
        )
        for snap in snap_result.scalars().all():
            # primero visto == más reciente por el order_by desc
            if snap.unit_id not in snapshots_by_unit:
                snapshots_by_unit[snap.unit_id] = snap

    # 5. Armar estructura de salida.
    fields_out: list[dict[str, Any]] = []
    total_area = 0.0
    total_paddocks = 0
    highest_risk_field: str | None = None
    highest_risk_score = float("-inf")
    fields_in_alert = 0

    for f in fields:
        n_paddocks = paddock_counts.get(f.id, 0)
        total_paddocks += n_paddocks
        area_ha = float(f.area_ha) if f.area_ha is not None else 0.0
        total_area += area_ha

        unit_id = f"user-field-{f.id}"
        snap = snapshots_by_unit.get(unit_id)
        risk_score = float(snap.risk_score) if snap and snap.risk_score is not None else None
        state = snap.state if snap else None
        state_level = int(snap.state_level) if snap and snap.state_level is not None else 0

        if state_level >= 2:
            fields_in_alert += 1

        if risk_score is not None and risk_score > highest_risk_score:
            highest_risk_score = risk_score
            highest_risk_field = f.id

        fields_out.append(
            {
                "field_id": f.id,
                "field_name": f.name,
                "department": f.department,
                "area_ha": area_ha,
                "n_paddocks": n_paddocks,
                "risk_score": risk_score,
                "state": state,
            }
        )

    return {
        "establishment_id": establishment_id,
        "establishment_name": est.name,
        "user_id": est.user_id,
        "fields": fields_out,
        "total_fields": len(fields),
        "total_paddocks": total_paddocks,
        "total_area_ha": total_area,
        "highest_risk_field": highest_risk_field,
        "fields_in_alert": fields_in_alert,
    }
