"""Fase 4: generacion de videos temporales por campo.

Este modulo es un stub minimo que otros agentes completaran con la logica
real (ffmpeg + snapshots de field_snapshots). Expone la API publica que
los endpoints y el worker loop consumen:

- FIELD_VIDEO_DIR: path donde se guardan los mp4.
- generate_field_video(db, job_id): procesa un FieldVideoJob queued.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.field_video import FieldVideoJob


logger = logging.getLogger(__name__)

FIELD_VIDEO_DIR = Path(__file__).resolve().parents[2] / ".tile_cache" / "field_videos"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _job_video_path(job_id: str) -> Path:
    FIELD_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    return FIELD_VIDEO_DIR / f"{job_id}.mp4"


async def generate_field_video(db: AsyncSession, job_id: str) -> dict | None:
    """Procesa un FieldVideoJob.

    Este stub marca el job como failed con un mensaje explicativo si la
    implementacion real (ffmpeg) aun no esta disponible. Otros agentes
    reemplazaran el cuerpo con la logica real sin cambiar la firma.
    """
    job = await db.get(FieldVideoJob, job_id)
    if job is None:
        return None

    job.status = "running"
    job.started_at = _now_utc()
    job.progress_pct = 0.0
    await db.commit()

    try:
        target = _job_video_path(job_id)
        # Stub: no hay render todavia. Dejamos el path apuntando al archivo
        # vacio esperado para que el endpoint de streaming devuelva 404
        # controlado hasta que el agente de render implemente la funcion.
        job.video_path = str(target)
        job.progress_pct = 100.0
        job.status = "failed"
        job.error_message = "render pending (fase 4 worker stub)"
        job.finished_at = _now_utc()
        await db.commit()
        return {"job_id": job.id, "status": job.status}
    except Exception as exc:  # pragma: no cover - defensive
        job.status = "failed"
        job.error_message = f"{type(exc).__name__}: {exc}"
        job.finished_at = _now_utc()
        await db.commit()
        logger.exception("generate_field_video fallo para %s", job_id)
        return {"job_id": job.id, "status": "failed"}
