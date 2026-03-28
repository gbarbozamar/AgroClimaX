from __future__ import annotations

import asyncio
import os
import logging

from app.bootstrap import initialize_application_state, run_startup_warmup
from app.core.config import settings
from app.db.session import engine
from app.services.pipeline_ops import scheduler_loop


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


async def main() -> None:
    logger.info("AgroClimaX worker iniciando - entorno=%s role=%s", settings.app_env, settings.app_runtime_role)
    await initialize_application_state()
    health_server = await _start_health_server()

    try:
        if settings.pipeline_startup_warmup_enabled:
            await run_startup_warmup()

        if settings.pipeline_scheduler_enabled:
            await scheduler_loop()
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
