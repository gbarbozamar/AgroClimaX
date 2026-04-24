"""
Endpoints para exponer las geometrías de scope al frontend.

Dos routers:
  - `public_router`: uruguay / departamento / seccion (sin auth).
  - `protected_router`: field (requiere session + ownership check).

El frontend usa estos datos para dibujar la máscara visual del mapa y
para saber los bbox al hacer fit al polígono del scope activo.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from shapely.geometry import mapping
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services import aoi_tile_clip
from app.services.auth import AuthContext, require_auth_context

logger = logging.getLogger(__name__)

# Router público: uruguay / departamento / seccion (sin auth).
public_router = APIRouter(tags=["geo-scopes"])
# Router protegido: field (ownership check, requiere session).
protected_router = APIRouter(tags=["geo-scopes"])


def _feature(geom, properties: dict) -> dict:
    return {
        "type": "Feature",
        "geometry": mapping(geom),
        "properties": properties,
    }


def _featurecollection(geom, properties: dict) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [_feature(geom, properties)],
    }


@public_router.get("/geojson/uruguay")
async def get_uruguay_geojson(db: AsyncSession = Depends(get_db)) -> dict:
    """Silueta del país (unión de 19 departamentos). Cacheada en memoria."""
    try:
        geom = await aoi_tile_clip.resolve_scope_geometry(db, "nacional", None)
    except aoi_tile_clip.ScopeNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return _featurecollection(geom, {"scope": "nacional", "ref": "uruguay"})


@public_router.get("/geojson/departamento/{ref}")
async def get_departamento_geojson(
    ref: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Polígono de un departamento por nombre."""
    try:
        geom = await aoi_tile_clip.resolve_scope_geometry(db, "departamento", ref)
    except aoi_tile_clip.ScopeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _featurecollection(geom, {"scope": "departamento", "ref": ref})


@public_router.get("/geojson/seccion/{ref}")
async def get_seccion_geojson(
    ref: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Polígono de una sección policial por unit_id."""
    try:
        geom = await aoi_tile_clip.resolve_scope_geometry(db, "seccion", ref)
    except aoi_tile_clip.ScopeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _featurecollection(geom, {"scope": "seccion", "ref": ref})


# Alias legacy `/geojson/{scope}/{ref}` para no romper frontends existentes.
# Redirige a los handlers específicos y rechaza explicitamente `field`.
@public_router.get("/geojson/{scope}/{ref}")
async def get_scope_geojson_legacy(
    scope: str,
    ref: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    if scope == "field":
        raise HTTPException(
            status_code=403,
            detail="Field scope requires authentication. Use /geojson/field/{field_id}.",
        )
    if scope == "departamento":
        return await get_departamento_geojson(ref, db=db)
    if scope == "seccion":
        return await get_seccion_geojson(ref, db=db)
    if scope == "nacional":
        return await get_uruguay_geojson(db=db)
    raise HTTPException(status_code=400, detail=f"Unsupported scope: {scope}")


@protected_router.get("/geojson/field/{field_id}")
async def get_field_geojson(
    field_id: str,
    db: AsyncSession = Depends(get_db),
    auth: AuthContext = Depends(require_auth_context),
) -> dict:
    """Polígono de un farm field. Sólo owner puede consultarlo."""
    try:
        geom = await aoi_tile_clip.resolve_scope_geometry(
            db, "field", field_id, user_id=auth.user.id,
        )
    except aoi_tile_clip.ScopeAuthError as exc:
        logger.warning("Unauthorized field geojson request user=%s field=%s", auth.user.id, field_id)
        raise HTTPException(status_code=403, detail=str(exc))
    except aoi_tile_clip.ScopeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _featurecollection(geom, {"scope": "field", "ref": field_id})
