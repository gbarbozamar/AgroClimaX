from __future__ import annotations

import asyncio
import logging

from app.bootstrap import initialize_application_state, run_startup_warmup
from app.core.config import settings
from app.db.session import engine
from app.services.pipeline_ops import scheduler_loop


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("AgroClimaX worker iniciando - entorno=%s role=%s", settings.app_env, settings.app_runtime_role)
    await initialize_application_state()

    try:
        if settings.pipeline_startup_warmup_enabled:
            await run_startup_warmup()

        if settings.pipeline_scheduler_enabled:
            await scheduler_loop()
        else:
            logger.info("Worker finalizado: scheduler deshabilitado")
    finally:
        await engine.dispose()
        logger.info("AgroClimaX worker cerrando")


if __name__ == "__main__":
    asyncio.run(main())
