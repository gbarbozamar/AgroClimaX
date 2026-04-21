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
# protected_router se incluye al final (línea abajo) después de colgarle routers.
# Aquí incluimos los routers públicos; el field-scope protegido queda dentro de
# protected_router (abajo). El orden relativo al public legacy `/geojson/{scope}/{ref}`
# importa: debemos montar el protected PRIMERO en api_router (antes que api_router
# incluya el legacy). Como `api_router.include_router(protected_router)` está al
# final del archivo, usamos este mount explícito para el protected de geo_scopes
# fuera del umbrella protected_router (tiene su propio Depends require_auth_context
# en el handler, así que no necesita el wrapper con dependencies=).
api_router.include_router(geo_scopes.protected_router)
api_router.include_router(geo_scopes.public_router)
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
