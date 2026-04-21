"""
Fase 5 — MCP feed router.

Endpoints protegidos por `require_service_token` (X-Service-Token header).
Aceptan opcionalmente X-User-Id para scopear queries al owner.

Estos endpoints alimentan al MCP server en apps/mcp/server.py (que invoca
Claude Desktop u otros clientes MCP). El service token se configura vía
env MCP_SERVICE_TOKEN.

Endpoints:
  GET  /api/v1/mcp/fields/{field_id}/snapshot?layer=...&date=...
  GET  /api/v1/mcp/fields/{field_id}/timeline?layer=...&days=...
  POST /api/v1/mcp/fields/{field_id}/video
  GET  /api/v1/mcp/fields/by-alert?min_level=N
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.farm import FarmField
from app.services.auth import require_service_token

router = APIRouter(prefix="/mcp", tags=["mcp"])


class VideoRequestBody(BaseModel):
    layer_key: str = "ndvi"
    duration_days: int = 30


def _field_ownership_filter(user_id: str | None):
    """Si X-User-Id viene, filtramos por él; sino devolvemos todo (admin service)."""
    if user_id:
        return FarmField.user_id == user_id
    return None


@router.get("/fields/{field_id}/snapshot")
async def mcp_get_snapshot(
    field_id: str,
    layer: str = Query("ndvi"),
    date_str: str | None = Query(None, alias="date"),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    try:
        from app.models.field_snapshot import FieldImageSnapshot
    except Exception:
        raise HTTPException(status_code=503, detail="FieldImageSnapshot model not available")

    # Ownership filter si X-User-Id fue provisto.
    user_id = auth.get("user_id")
    if user_id:
        field_row = (await db.execute(
            select(FarmField).where(FarmField.id == field_id, FarmField.user_id == user_id)
        )).scalar_one_or_none()
        if field_row is None:
            raise HTTPException(status_code=404, detail="Field not found or not owned by user")

    stmt = select(FieldImageSnapshot).where(
        FieldImageSnapshot.field_id == field_id,
        FieldImageSnapshot.layer_key == layer,
    )
    if date_str:
        try:
            target = date.fromisoformat(date_str)
            stmt = stmt.where(FieldImageSnapshot.observed_at == target)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format (expect YYYY-MM-DD)")
    else:
        stmt = stmt.order_by(desc(FieldImageSnapshot.observed_at)).limit(1)

    row = (await db.execute(stmt)).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return {
        "field_id": field_id,
        "layer_key": layer,
        "observed_at": row.observed_at.isoformat(),
        "image_url": f"/api/v1/campos/{field_id}/snapshots/{row.storage_key}",
        "metadata": {
            "risk_score": row.risk_score,
            "confidence_score": row.confidence_score,
            "s1_humidity_mean_pct": row.s1_humidity_mean_pct,
            "s2_ndmi_mean": row.s2_ndmi_mean,
            "spi_30d": row.spi_30d,
            "area_ha": row.area_ha,
            "bbox": row.bbox_json,
        },
    }


@router.get("/fields/{field_id}/timeline")
async def mcp_get_timeline(
    field_id: str,
    layer: str = Query("ndvi"),
    days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    try:
        from app.models.field_snapshot import FieldImageSnapshot
    except Exception:
        raise HTTPException(status_code=503, detail="FieldImageSnapshot model not available")

    cutoff = date.today() - timedelta(days=days)
    stmt = (
        select(FieldImageSnapshot)
        .where(FieldImageSnapshot.field_id == field_id)
        .where(FieldImageSnapshot.layer_key == layer)
        .where(FieldImageSnapshot.observed_at >= cutoff)
        .order_by(FieldImageSnapshot.observed_at.asc())
    )
    rows = (await db.execute(stmt)).scalars().all()
    return {
        "field_id": field_id,
        "layer_key": layer,
        "total": len(rows),
        "days": [
            {
                "observed_at": r.observed_at.isoformat(),
                "image_url": f"/api/v1/campos/{field_id}/snapshots/{r.storage_key}",
                "risk_score": r.risk_score,
                "ndmi_mean": r.s2_ndmi_mean,
            }
            for r in rows
        ],
    }


@router.post("/fields/{field_id}/video")
async def mcp_request_video(
    field_id: str,
    body: VideoRequestBody,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    try:
        from app.models.field_video import FieldVideoJob
    except Exception:
        raise HTTPException(status_code=503, detail="FieldVideoJob model not available")

    from sqlalchemy import and_

    # Idempotencia: si hay un job ready o queued reciente, devolvemos el existente.
    recent = (await db.execute(
        select(FieldVideoJob)
        .where(and_(
            FieldVideoJob.field_id == field_id,
            FieldVideoJob.layer_key == body.layer_key,
            FieldVideoJob.duration_days == body.duration_days,
        ))
        .order_by(desc(FieldVideoJob.created_at))
        .limit(1)
    )).scalar_one_or_none()
    if recent is not None and recent.status in ("queued", "rendering", "ready"):
        return {
            "job_id": recent.id,
            "status": recent.status,
            "reused": True,
        }

    # Crear job nuevo (worker lo procesará).
    user_id = auth.get("user_id") or "mcp-service"
    job = FieldVideoJob(
        id=str(uuid4()),
        field_id=field_id,
        user_id=user_id,
        layer_key=body.layer_key,
        duration_days=body.duration_days,
        status="queued",
    )
    db.add(job)
    await db.commit()
    return {"job_id": job.id, "status": job.status, "reused": False}


@router.get("/fields/by-alert")
async def mcp_list_fields_by_alert(
    min_level: int = Query(2, ge=0, le=4),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> list[dict[str, Any]]:
    try:
        from app.models.materialized import UnitIndexSnapshot
    except Exception:
        return []

    user_id = auth.get("user_id")

    # Query last snapshot per field_id en UnitIndexSnapshot (si existe columna
    # field_id); sino fallback a AlertState. Hacemos el filter en Python por
    # simplicidad (cantidad esperada pequeña).
    fields_stmt = select(FarmField)
    if user_id:
        fields_stmt = fields_stmt.where(FarmField.user_id == user_id)
    fields = (await db.execute(fields_stmt)).scalars().all()

    results: list[dict[str, Any]] = []
    for field in fields:
        # Intentar buscar snapshot por field.unit_id (si los fields están ligados
        # a AOIUnit via user-field unit_id). Si no, skip.
        unit_id = getattr(field, "unit_id", None) or f"user-field-{field.id}"
        snap = (await db.execute(
            select(UnitIndexSnapshot)
            .where(UnitIndexSnapshot.unit_id == unit_id)
            .order_by(desc(UnitIndexSnapshot.observed_at))
            .limit(1)
        )).scalar_one_or_none()
        level = getattr(snap, "state_level", None) if snap else None
        if level is not None and level >= min_level:
            results.append({
                "field_id": field.id,
                "field_name": field.name,
                "department": field.department,
                "state_level": level,
                "risk_score": getattr(snap, "risk_score", None),
                "observed_at": snap.observed_at.isoformat() if snap.observed_at else None,
            })
    return results
