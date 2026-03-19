from fastapi import APIRouter
from app.api.v1.endpoints import alertas, humedad, pipeline

api_router = APIRouter()

api_router.include_router(alertas.router)
api_router.include_router(humedad.router)
api_router.include_router(pipeline.router)
