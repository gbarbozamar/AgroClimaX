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
            job.error_message = (
                f"insufficient_frames: {len(rows)} snapshots reales en los ultimos "
                f"{job.duration_days} dias (se necesitan >=2)"
            )
            job.finished_at = _now_utc()
            await db.commit()
            return {"job_id": job.id, "status": "failed"}

        # Mantener pairing (path, observed_at) para poder calcular cobertura real
        # aun despues de filtrar archivos ausentes en disco.
        pairs: list[tuple[Path, date]] = []
        for row in rows:
            if row.storage_key.startswith("fields/"):
                p = FIELD_SNAPSHOT_ROOT / row.storage_key.replace("fields/", "", 1)
            else:
                p = Path(row.storage_key)
            pairs.append((p, row.observed_at))
        existing_pairs = [(p, d) for (p, d) in pairs if p.exists()]
        existing = [p for (p, _) in existing_pairs]

        if len(existing) < 2:
            job.status = "failed"
            job.error_message = (
                f"insufficient_frames: {len(existing)} snapshots reales en los ultimos "
                f"{job.duration_days} dias (se necesitan >=2)"
            )
            job.finished_at = _now_utc()
            await db.commit()
            return {"job_id": job.id, "status": "failed"}

        # Cobertura real: rango entre primer y ultimo frame que SI existe.
        dates_sorted = sorted(d for (_, d) in existing_pairs)
        covered_days = max(0, (dates_sorted[-1] - dates_sorted[0]).days)

        # fps adaptativo: para videos con pocos frames bajar el fps para que el
        # usuario pueda leer cada escena. Default 4, minimo 1.
        adaptive_fps = max(1, min(DEFAULT_FPS, len(existing) // 4))

        target = _job_video_path(job_id)
        # Encoding blocking → to_thread para no bloquear el loop asyncio.
        frame_count = await asyncio.to_thread(_encode_mp4_sync, existing, target, adaptive_fps)

        job.video_path = str(target)
        # Garantizar consistencia entre lo que devolvio el encoder (puede skipear
        # frames corruptos con PIL) y el contador persistido.
        try:
            setattr(job, "frame_count", int(frame_count))
        except Exception:
            pass
        # Persistir cobertura real. Intentar columna dedicada si existe; sino
        # guardar como info informativa en error_message (no es realmente un error).
        stored_actual = False
        try:
            if hasattr(job, "duration_days_actual"):
                setattr(job, "duration_days_actual", covered_days)
                stored_actual = True
        except Exception:
            stored_actual = False
        if not stored_actual:
            job.error_message = (
                f"covers {covered_days} real days out of {job.duration_days} requested"
            )
        job.progress_pct = 100.0
        job.status = "ready"
        job.finished_at = _now_utc()
        await db.commit()
        logger.info(
            "field video ready job=%s frames=%d covered=%dd fps=%d path=%s",
            job_id, frame_count, covered_days, adaptive_fps, target,
        )
        return {
            "job_id": job.id,
            "status": "ready",
            "frame_count": frame_count,
            "covered_days": covered_days,
            "requested_days": job.duration_days,
            "fps": adaptive_fps,
        }

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
