"""Fase 4: endpoints para jobs de video temporal por campo.

Endpoints:
- POST /api/v1/campos/{field_id}/videos
- GET  /api/v1/campos/{field_id}/videos
- GET  /api/v1/campos/{field_id}/videos/{job_id}
- GET  /api/v1/campos/{field_id}/videos/{job_id}/file

Todos validan ownership del field via FarmField.user_id.
Reusa jobs con status=ready creados hace < 24h para evitar re-render.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.farm import FarmField
from app.models.field_video import FieldVideoJob
from app.services.auth import AuthContext, require_auth_context


logger = logging.getLogger(__name__)

router = APIRouter(tags=["field_videos"])

ALLOWED_LAYER_KEYS = {
    "ndvi",
    "ndmi",
    "alerta_fusion",
    "humedad",
    "precip",
    "temperatura",
}
MIN_DURATION_DAYS = 1
MAX_DURATION_DAYS = 180
READY_TTL_HOURS = 24


class VideoJobCreateRequest(BaseModel):
    layer_key: str = Field(..., min_length=1, max_length=64)
    duration_days: int = Field(30, ge=MIN_DURATION_DAYS, le=MAX_DURATION_DAYS)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _ensure_field_owned(db: AsyncSession, *, user_id: str, field_id: str) -> FarmField:
    row = await db.get(FarmField, field_id)
    if row is None or row.user_id != user_id or not row.active:
        raise HTTPException(status_code=404, detail="Campo no encontrado")
    return row


_COVERED_RE = None  # lazy-compiled inside _covered_from_error


def _covered_from_error(msg: str | None) -> int | None:
    """Extrae `covers N real days out of M requested` si el modelo no tiene
    columna dedicada y usamos error_message como fallback informativo."""
    global _COVERED_RE
    if not msg or "covers " not in msg:
        return None
    import re
    if _COVERED_RE is None:
        _COVERED_RE = re.compile(r"covers\s+(\d+)\s+real days")
    m = _COVERED_RE.search(msg)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _serialize(job: FieldVideoJob) -> dict[str, Any]:
    # Derivar size_bytes del archivo en disco (autoritativo). frame_count se
    # guarda en la columna del modelo si existe, sino queda None.
    size_bytes: int | None = None
    try:
        if job.video_path:
            p = Path(job.video_path)
            if p.exists():
                size_bytes = p.stat().st_size
    except Exception:
        size_bytes = None
    frame_count = getattr(job, "frame_count", None)

    # covered_days: preferir columna dedicada, sino parsear del error_message
    # (lo guardamos como info positiva cuando no hay columna).
    covered_days: int | None = getattr(job, "duration_days_actual", None)
    if covered_days is None and job.status == "ready":
        covered_days = _covered_from_error(job.error_message)

    return {
        "job_id": job.id,
        "field_id": job.field_id,
        "layer_key": job.layer_key,
        "duration_days": job.duration_days,
        "requested_days": job.duration_days,
        "covered_days": covered_days,
        "status": job.status,
        "progress_pct": round(float(job.progress_pct or 0.0), 2),
        "video_url": (
            f"/api/v1/campos/{job.field_id}/videos/{job.id}/file"
            if job.status == "ready"
            else None
        ),
        "frame_count": frame_count,
        "size_bytes": size_bytes,
        "error_message": job.error_message,
        "created_at": _coerce_utc(job.created_at).isoformat() if job.created_at else None,
        "finished_at": _coerce_utc(job.finished_at).isoformat() if job.finished_at else None,
    }


@router.post("/campos/{field_id}/videos", status_code=202)
async def create_field_video_job(
    field_id: str,
    payload: VideoJobCreateRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_field_owned(db, user_id=auth.user.id, field_id=field_id)

    layer_key = payload.layer_key.strip().lower()
    if layer_key not in ALLOWED_LAYER_KEYS:
        raise HTTPException(status_code=422, detail=f"layer_key no soportado: {layer_key}")

    cutoff = _now_utc() - timedelta(hours=READY_TTL_HOURS)
    existing_stmt = (
        select(FieldVideoJob)
        .where(
            FieldVideoJob.field_id == field_id,
            FieldVideoJob.layer_key == layer_key,
            FieldVideoJob.duration_days == payload.duration_days,
            FieldVideoJob.status == "ready",
        )
        .order_by(desc(FieldVideoJob.created_at))
        .limit(1)
    )
    existing = (await db.execute(existing_stmt)).scalar_one_or_none()
    if existing is not None:
        existing_created = _coerce_utc(existing.created_at)
        if existing_created and existing_created >= cutoff:
            return _serialize(existing)

    job = FieldVideoJob(
        field_id=field_id,
        user_id=auth.user.id,
        layer_key=layer_key,
        duration_days=payload.duration_days,
        status="queued",
        progress_pct=0.0,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return _serialize(job)


@router.get("/campos/{field_id}/videos")
async def list_field_video_jobs(
    field_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_field_owned(db, user_id=auth.user.id, field_id=field_id)
    stmt = (
        select(FieldVideoJob)
        .where(FieldVideoJob.field_id == field_id)
        .order_by(desc(FieldVideoJob.created_at))
        .limit(100)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return {"total": len(rows), "items": [_serialize(r) for r in rows]}


@router.get("/campos/{field_id}/videos/{job_id}")
async def get_field_video_job(
    field_id: str,
    job_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_field_owned(db, user_id=auth.user.id, field_id=field_id)
    job = await db.get(FieldVideoJob, job_id)
    if job is None or job.field_id != field_id or job.user_id != auth.user.id:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    return _serialize(job)


@router.get("/campos/{field_id}/videos/{job_id}/file")
async def download_field_video(
    field_id: str,
    job_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_field_owned(db, user_id=auth.user.id, field_id=field_id)
    job = await db.get(FieldVideoJob, job_id)
    if job is None or job.field_id != field_id or job.user_id != auth.user.id:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    if job.status != "ready" or not job.video_path:
        raise HTTPException(status_code=409, detail=f"Video no disponible (status={job.status})")
    path = Path(job.video_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Archivo de video no encontrado en disco")
    return FileResponse(
        path=str(path),
        media_type="video/mp4",
        filename=f"{field_id}_{job.layer_key}_{job.duration_days}d.mp4",
    )
