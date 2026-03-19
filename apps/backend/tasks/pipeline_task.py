"""
Tarea Celery — Pipeline diario Copernicus.
Se ejecuta automáticamente según PIPELINE_CRON_HOUR en settings.
"""
from celery import Celery
from app.core.config import settings

celery_app = Celery("agroclimax", broker=settings.redis_url, backend=settings.redis_url)

celery_app.conf.beat_schedule = {
    "pipeline-diario-copernicus": {
        "task": "tasks.pipeline_task.run_pipeline_diario",
        "schedule": {
            "hour": settings.pipeline_cron_hour,
            "minute": settings.pipeline_cron_minute,
        },
    }
}


@celery_app.task(name="tasks.pipeline_task.run_pipeline_diario", bind=True, max_retries=3)
def run_pipeline_diario(self):
    """Ejecuta el pipeline Copernicus completo y evalúa alertas."""
    import logging
    logger = logging.getLogger(__name__)

    try:
        from app.copernicus.pipeline import ejecutar_pipeline
        resultado = ejecutar_pipeline()
        logger.info("Pipeline completado: %s", resultado.resumen())
        return resultado.resumen()
    except Exception as exc:
        logger.error("Error en pipeline: %s", exc)
        raise self.retry(exc=exc, countdown=60 * 30)  # reintentar en 30 min
