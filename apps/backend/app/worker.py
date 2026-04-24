from __future__ import annotations

import asyncio
import os
import logging

from sqlalchemy import asc, select

from app.bootstrap import initialize_application_state, run_startup_warmup
from app.core.config import settings
from app.db.session import AsyncSessionLocal, engine
from app.services.pipeline_ops import scheduler_loop


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FIELD_VIDEO_POLL_SECONDS = 10


async def _handle_healthcheck(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        with asyncio.timeout(2):
            await reader.readuntil(b"\r\n\r\n")
    except Exception:
        pass

    body = b'{"status":"ok","runtime":"app.worker"}'
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"content-type: application/json\r\n"
        + f"content-length: {len(body)}\r\n".encode("ascii")
        + b"connection: close\r\n\r\n"
        + body
    )
    writer.write(response)
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def _start_health_server() -> asyncio.AbstractServer | None:
    port_value = os.getenv("PORT", "").strip()
    if settings.app_runtime_role != "worker" or not port_value:
        return None
    try:
        port = int(port_value)
    except ValueError:
        logger.warning("PORT invalido para healthcheck del worker: %s", port_value)
        return None
    server = await asyncio.start_server(_handle_healthcheck, host="0.0.0.0", port=port)
    logger.info("Worker healthcheck escuchando en 0.0.0.0:%s", port)
    return server


async def field_video_loop() -> None:
    """Fase 4: procesa FieldVideoJob queued uno a la vez (concurrencia 1)."""
    try:
        from app.models.field_video import FieldVideoJob
        from app.services.field_video import generate_field_video
    except Exception:  # pragma: no cover - model/service no listo
        logger.warning("FieldVideo loop deshabilitado: modulos no disponibles")
        return

    while True:
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    select(FieldVideoJob)
                    .where(FieldVideoJob.status == "queued")
                    .order_by(asc(FieldVideoJob.created_at))
                    .limit(1)
                )
                job = (await session.execute(stmt)).scalar_one_or_none()
                if job is not None:
                    logger.info("FieldVideo worker tomando job %s", job.id)
                    await generate_field_video(session, job.id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Fallo en field_video_loop")
        await asyncio.sleep(FIELD_VIDEO_POLL_SECONDS)


async def main() -> None:
    logger.info("AgroClimaX worker iniciando - entorno=%s role=%s", settings.app_env, settings.app_runtime_role)
    await initialize_application_state()
    health_server = await _start_health_server()

    try:
        if settings.pipeline_startup_warmup_enabled:
            await run_startup_warmup()

        if settings.pipeline_scheduler_enabled:
            await asyncio.gather(
                scheduler_loop(),
                field_video_loop(),
            )
        else:
            logger.info("Worker finalizado: scheduler deshabilitado")
    finally:
        if health_server is not None:
            health_server.close()
            await health_server.wait_closed()
        await engine.dispose()
        logger.info("AgroClimaX worker cerrando")


if __name__ == "__main__":
    asyncio.run(main())
