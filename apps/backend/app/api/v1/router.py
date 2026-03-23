from fastapi import APIRouter

from app.api.v1.endpoints import (
    alertas,
    ground_truth,
    legacy,
    notifications,
    pipeline,
    public,
    unidades,
)

api_router = APIRouter()

api_router.include_router(alertas.router)
api_router.include_router(unidades.router)
api_router.include_router(ground_truth.router)
api_router.include_router(notifications.router)
api_router.include_router(pipeline.router)
api_router.include_router(public.router)
api_router.include_router(legacy.router)
