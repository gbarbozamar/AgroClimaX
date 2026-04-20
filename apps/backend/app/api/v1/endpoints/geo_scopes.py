"""
Endpoints públicos para exponer las geometrías de scope al frontend.

El frontend usa estos datos para dibujar la máscara visual del mapa
(cubrir con negro lo que está fuera del scope activo) y para saber
los bbox al hacer fit.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from shapely.geometry import mapping
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services import aoi_tile_clip

logger = logging.getLogger(__name__)
router = APIRouter(tags=["geo-scopes"])


def _feature(geom, properties: dict) -> dict:
    return {
        "type": "Feature",
        "geometry": mapping(geom),
        "properties": properties,
    }


@router.get("/geojson/uruguay")
async def get_uruguay_geojson(db: AsyncSession = Depends(get_db)) -> dict:
    """Silueta del país (unión de 19 departamentos). Cacheada en memoria."""
    try:
        geom = await aoi_tile_clip.resolve_scope_geometry(db, "nacional", None)
    except aoi_tile_clip.ScopeNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {
        "type": "FeatureCollection",
        "features": [_feature(geom, {"scope": "nacional", "ref": "uruguay"})],
    }


@router.get("/geojson/{scope}/{ref}")
async def get_scope_geojson(
    scope: str,
    ref: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Devuelve el polígono de un scope arbitrario. `scope=field` requiere auth."""
    if scope == "field":
        raise HTTPException(
            status_code=403,
            detail="Field scope not exposed via public endpoint. Use authenticated client.",
        )
    try:
        geom = await aoi_tile_clip.resolve_scope_geometry(db, scope, ref)
    except aoi_tile_clip.ScopeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "type": "FeatureCollection",
        "features": [_feature(geom, {"scope": scope, "ref": ref})],
    }
