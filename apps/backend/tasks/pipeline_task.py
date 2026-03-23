"""
Tarea Celery para ingesta diaria y recalibracion semanal.
"""
from celery import Celery
from celery.schedules import crontab

from app.core.config import settings


celery_app = Celery("agroclimax", broker=settings.redis_url, backend=settings.redis_url)

celery_app.conf.beat_schedule = {
    "pipeline-diario-nacional": {
        "task": "tasks.pipeline_task.run_pipeline_nacional",
        "schedule": crontab(hour=settings.pipeline_cron_hour, minute=settings.pipeline_cron_minute),
    },
    "recalibracion-semanal": {
        "task": "tasks.pipeline_task.run_recalibration",
        "schedule": crontab(hour=settings.pipeline_cron_hour, minute=settings.pipeline_cron_minute, day_of_week="mon"),
    },
}


@celery_app.task(name="tasks.pipeline_task.run_pipeline_nacional", bind=True, max_retries=3)
def run_pipeline_nacional(self):
    import asyncio

    from app.db.session import AsyncSessionLocal
    from app.services.analysis import run_daily_pipeline

    async def _run():
        async with AsyncSessionLocal() as session:
            return await run_daily_pipeline(session)

    try:
        return asyncio.run(_run())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * 30)


@celery_app.task(name="tasks.pipeline_task.run_recalibration", bind=True, max_retries=2)
def run_recalibration(self):
    import asyncio

    from app.db.session import AsyncSessionLocal
    from app.services.analysis import recompute_calibrations

    async def _run():
        async with AsyncSessionLocal() as session:
            return await recompute_calibrations(session)

    try:
        return asyncio.run(_run())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * 15)
