"""Fase 4: generación real de videos timelapse por campo.

Lee FieldImageSnapshot para (field, layer) en ventana de N días, compone
un MP4 H.264 fps=4 con imageio-ffmpeg y persiste el path en FieldVideoJob.

Si hay menos de 2 frames o imageio-ffmpeg no está disponible, marca el
job como failed con error_message explicativo (no crashea el worker).
"""
from __future__ import annotations

import asyncio
import io
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.field_video import FieldVideoJob


logger = logging.getLogger(__name__)

FIELD_VIDEO_DIR = Path(__file__).resolve().parents[2] / ".tile_cache" / "field_videos"
# Misma raíz que usa field_snapshots para leer los PNGs fuente.
FIELD_SNAPSHOT_ROOT = Path(__file__).resolve().parents[2] / ".tile_cache" / "fields"

DEFAULT_FPS = 4


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _job_video_path(job_id: str) -> Path:
    FIELD_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    return FIELD_VIDEO_DIR / f"{job_id}.mp4"


def _encode_mp4_sync(png_paths: list[Path], target: Path, fps: int = DEFAULT_FPS) -> int:
    """Blocking encode helper — se corre con asyncio.to_thread."""
    try:
        import imageio.v3 as iio
        from PIL import Image
    except Exception as exc:  # pragma: no cover - depends on env
        raise RuntimeError(f"imageio or PIL missing: {exc}") from exc

    frames = []
    for p in png_paths:
        try:
            with Image.open(p) as im:
                frames.append(im.convert("RGB").copy())
        except Exception as exc:
            logger.warning("skip frame %s: %s", p, exc)
    if len(frames) < 2:
        raise RuntimeError("insufficient_frames")
    # Usar el primer frame como referencia de tamaño; resize otros si difieren.
    w, h = frames[0].size
    normalized = [f if f.size == (w, h) else f.resize((w, h)) for f in frames]
    target.parent.mkdir(parents=True, exist_ok=True)
    # imageio.v3.imwrite con plugin pyav/ffmpeg arma MP4 H.264.
    iio.imwrite(target, [list(im.getdata()) and __import__("numpy").array(im) for im in normalized], fps=fps, codec="libx264")
    return len(normalized)


async def generate_field_video(db: AsyncSession, job_id: str) -> dict | None:
    """Procesa un FieldVideoJob queued: compila PNGs → MP4 timelapse."""
    job = await db.get(FieldVideoJob, job_id)
    if job is None:
        return None

    job.status = "running"
    job.started_at = _now_utc()
    job.progress_pct = 0.0
    await db.commit()

    try:
        # Lazy import — FieldImageSnapshot puede no estar disponible en entornos
        # de testing sin Fase 2 materializada.
        try:
            from app.models.field_snapshot import FieldImageSnapshot
        except Exception as exc:
            raise RuntimeError(f"FieldImageSnapshot model missing: {exc}")

        cutoff = date.today() - timedelta(days=job.duration_days)
        stmt = (
            select(FieldImageSnapshot)
            .where(FieldImageSnapshot.field_id == job.field_id)
            .where(FieldImageSnapshot.layer_key == job.layer_key)
            .where(FieldImageSnapshot.observed_at >= cutoff)
            .order_by(FieldImageSnapshot.observed_at.asc())
        )
        rows = (await db.execute(stmt)).scalars().all()

        if len(rows) < 2:
            job.status = "failed"
            job.error_message = f"insufficient_frames ({len(rows)} found, need >= 2)"
            job.finished_at = _now_utc()
            await db.commit()
            return {"job_id": job.id, "status": "failed"}

        png_paths = [FIELD_SNAPSHOT_ROOT / row.storage_key.replace("fields/", "", 1)
                     if row.storage_key.startswith("fields/")
                     else Path(row.storage_key)
                     for row in rows]
        # Filter existing files
        existing = [p for p in png_paths if p.exists()]
        if len(existing) < 2:
            job.status = "failed"
            job.error_message = f"insufficient_frames on disk ({len(existing)} exist of {len(rows)})"
            job.finished_at = _now_utc()
            await db.commit()
            return {"job_id": job.id, "status": "failed"}

        target = _job_video_path(job_id)
        # Encoding blocking → to_thread para no bloquear el loop asyncio.
        frame_count = await asyncio.to_thread(_encode_mp4_sync, existing, target, DEFAULT_FPS)

        job.video_path = str(target)
        job.progress_pct = 100.0
        job.status = "ready"
        job.finished_at = _now_utc()
        await db.commit()
        logger.info("field video ready job=%s frames=%d path=%s", job_id, frame_count, target)
        return {"job_id": job.id, "status": "ready", "frame_count": frame_count}

    except Exception as exc:
        logger.exception("generate_field_video fallo para %s", job_id)
        job.status = "failed"
        job.error_message = f"{type(exc).__name__}: {exc}"
        job.finished_at = _now_utc()
        try:
            await db.commit()
        except Exception:
            await db.rollback()
        return {"job_id": job.id, "status": "failed"}
