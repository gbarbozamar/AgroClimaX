from fastapi import APIRouter, Depends

from app.api.v1.endpoints import (
    alert_subscriptions,
    alertas,
    auth,
    campos,
    client_diagnostics,
    geo_scopes,
    ground_truth,
    hexagonos,
    legacy,
    layers,
    notifications,
    pipeline,
    profile,
    productivas,
    public,
    sections,
    settings,
    unidades,
)
from app.services.auth import require_authenticated_request

api_router = APIRouter()
protected_router = APIRouter(dependencies=[Depends(require_authenticated_request)])

api_router.include_router(auth.router)
api_router.include_router(client_diagnostics.router)
api_router.include_router(geo_scopes.router)
api_router.include_router(alert_subscriptions.public_router)
protected_router.include_router(alertas.router)
protected_router.include_router(alert_subscriptions.router)
protected_router.include_router(campos.router)
protected_router.include_router(unidades.router)
protected_router.include_router(layers.router)
protected_router.include_router(hexagonos.router)
protected_router.include_router(sections.router)
protected_router.include_router(ground_truth.router)
protected_router.include_router(notifications.router)
protected_router.include_router(pipeline.router)
protected_router.include_router(productivas.router)
protected_router.include_router(profile.router)
protected_router.include_router(public.router)
protected_router.include_router(settings.router)
protected_router.include_router(legacy.router)
api_router.include_router(protected_router)
