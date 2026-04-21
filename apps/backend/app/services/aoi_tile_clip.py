"""
Clipping multi-nivel para tiles de Copernicus.

Cuatro scopes soportados:
  - "nacional"   -> unión de los 19 departamentos de Uruguay (cacheado en memoria)
  - "departamento" -> polígono del departamento (AOIUnit scope='departamento')
  - "seccion"    -> polígono de la sección policial (SpatialLayerFeature scope='seccion')
  - "field"      -> polígono del campo del usuario (FarmField, REQUIERE auth)

Uso:
    geom = await resolve_scope_geometry(session, "departamento", "Rivera")
    if not tile_intersects(z, x, y, geom):
        return TRANSPARENT_PNG  # early exit

    png_recortado = clip_png_tile_to_aoi(raw_png, z, x, y, geom)

Performance: shapely.prepared.prep() cachea una versión rápida para intersects
repetidos. El bbox envelope short-circuit evita rasterización en tiles disjuntos.
"""
from __future__ import annotations

import io
import logging
import math
from typing import Any, Optional

import numpy as np
from PIL import Image
from shapely.geometry import Polygon, shape
from shapely.ops import unary_union
from shapely.prepared import prep
from shapely.geometry.base import BaseGeometry
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.farm import FarmField
from app.models.humedad import AOIUnit
from app.models.materialized import SpatialLayerFeature


logger = logging.getLogger(__name__)


# ── Cache en memoria ──────────────────────────────────────────────────
_COUNTRY_GEOM: BaseGeometry | None = None
_SCOPE_CACHE: dict[tuple[str, str | None], BaseGeometry] = {}
_PREPARED_CACHE: dict[int, Any] = {}  # id(geom) -> prepared geom


_VALID_SCOPES = {"nacional", "departamento", "seccion", "field"}


class ScopeAuthError(Exception):
    """Auth violation: user trying to access someone else's field scope."""


class ScopeNotFoundError(Exception):
    """No geometry found for the requested scope/ref combination."""


# ── Helpers geométricos ───────────────────────────────────────────────

def tile_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Bbox WGS84 (lng_min, lat_min, lng_max, lat_max) de un tile XYZ."""
    n = 2 ** z
    lng_min = x / n * 360.0 - 180.0
    lng_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return (lng_min, lat_min, lng_max, lat_max)


def _tile_polygon(z: int, x: int, y: int) -> Polygon:
    lng_min, lat_min, lng_max, lat_max = tile_bbox(z, x, y)
    return Polygon([
        (lng_min, lat_min), (lng_max, lat_min),
        (lng_max, lat_max), (lng_min, lat_max),
        (lng_min, lat_min),
    ])


def _as_prepared(geom: BaseGeometry) -> Any:
    """Memoiza la versión prepared (acelera intersects en bulk)."""
    key = id(geom)
    cached = _PREPARED_CACHE.get(key)
    if cached is None:
        cached = prep(geom)
        _PREPARED_CACHE[key] = cached
    return cached


def tile_intersects(z: int, x: int, y: int, geom: BaseGeometry | None) -> bool:
    """True si el tile XYZ intersecta con la geometría (bbox short-circuit)."""
    if geom is None:
        return True  # sin scope = todo pasa
    tile = _tile_polygon(z, x, y)
    # Bbox de la geom como filtro rápido
    gminx, gminy, gmaxx, gmaxy = geom.bounds
    tminx, tminy, tmaxx, tmaxy = tile.bounds
    if tmaxx < gminx or tminx > gmaxx or tmaxy < gminy or tminy > gmaxy:
        return False
    return _as_prepared(geom).intersects(tile)


def tile_fully_contained(z: int, x: int, y: int, geom: BaseGeometry | None) -> bool:
    """True si el tile cae enteramente dentro de la geometría."""
    if geom is None:
        return False
    tile = _tile_polygon(z, x, y)
    return _as_prepared(geom).contains(tile)


# ── Resolución de geometrías por scope ────────────────────────────────

async def _resolve_country(session: AsyncSession) -> BaseGeometry:
    """Unión de los 19 departamentos de Uruguay, cacheada en memoria.

    Aplicamos un buffer ~0.05° (~5.5 km) al resultado para cerrar los slivers
    y recortes del este (Rocha / Treinta y Tres / Cerro Largo) donde los
    polígonos departamentales están truncados respecto a la línea de costa
    real. Con 0.01° el contorno aún dejaba un hueco visible sobre el Atlántico
    y la frontera brasileña a zoom >= 9. 0.05° es visualmente casi
    imperceptible a escala país pero elimina los gaps de clipping incluso a
    zoom alto.
    """
    global _COUNTRY_GEOM
    if _COUNTRY_GEOM is not None:
        return _COUNTRY_GEOM
    result = await session.execute(
        select(AOIUnit.geometry_geojson).where(
            AOIUnit.scope == "departamento",
            AOIUnit.active == True,  # noqa: E712
            AOIUnit.geometry_geojson.is_not(None),
        )
    )
    geojsons = [row[0] for row in result.all() if row[0]]
    if not geojsons:
        raise ScopeNotFoundError("No department geometries found to build country union")
    geoms = [shape(g) for g in geojsons]
    raw_union = unary_union(geoms)
    # Buffer ~0.05° (~5.5 km) cierra slivers y gaps costeros sin expandir
    # percepciblemente el contorno del país en overlays visuales.
    _COUNTRY_GEOM = raw_union.buffer(0.05)
    logger.info(
        "country union computed from %d departments (raw bounds=%s, buffered bounds=%s)",
        len(geoms), raw_union.bounds, _COUNTRY_GEOM.bounds,
    )
    return _COUNTRY_GEOM


async def _resolve_department(session: AsyncSession, name: str) -> BaseGeometry:
    """Polígono de un departamento por nombre (o slug)."""
    result = await session.execute(
        select(AOIUnit.geometry_geojson).where(
            AOIUnit.scope == "departamento",
            (AOIUnit.name == name) | (AOIUnit.slug == name) | (AOIUnit.department == name),
        ).limit(1)
    )
    row = result.first()
    if not row or not row[0]:
        raise ScopeNotFoundError(f"Department not found: {name}")
    return shape(row[0])


async def _resolve_seccion(session: AsyncSession, unit_id: str) -> BaseGeometry:
    """Polígono de una sección policial por unit_id."""
    result = await session.execute(
        select(SpatialLayerFeature.geometry_geojson).where(
            SpatialLayerFeature.layer_scope == "seccion",
            SpatialLayerFeature.unit_id == unit_id,
        ).limit(1)
    )
    row = result.first()
    if not row or not row[0]:
        raise ScopeNotFoundError(f"Police section not found: {unit_id}")
    return shape(row[0])


async def _resolve_field(
    session: AsyncSession, field_id: str, *, user_id: str | None
) -> BaseGeometry:
    """Polígono de un farm field con check de autorización."""
    if not user_id:
        raise ScopeAuthError("Field scope requires authenticated user")
    result = await session.execute(
        select(FarmField.field_geometry_geojson, FarmField.user_id).where(
            FarmField.id == field_id,
            FarmField.active == True,  # noqa: E712
        ).limit(1)
    )
    row = result.first()
    if not row:
        raise ScopeNotFoundError(f"Farm field not found: {field_id}")
    geometry_geojson, owner_id = row
    if owner_id != user_id:
        raise ScopeAuthError("User does not own this field")
    if not geometry_geojson:
        raise ScopeNotFoundError(f"Field has no geometry: {field_id}")
    return shape(geometry_geojson)


async def resolve_scope_geometry(
    session: AsyncSession,
    scope: str,
    ref: str | None,
    *,
    user_id: str | None = None,
) -> BaseGeometry:
    """Entry point para resolver cualquier scope. Cachea por (scope, ref)."""
    if scope not in _VALID_SCOPES:
        raise ValueError(f"Invalid scope: {scope}. Valid: {_VALID_SCOPES}")

    # Field scope nunca cacheado globalmente (es per-user, cambia con edits).
    if scope == "field":
        return await _resolve_field(session, ref, user_id=user_id)

    cache_key = (scope, ref)
    cached = _SCOPE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if scope == "nacional":
        geom = await _resolve_country(session)
    elif scope == "departamento":
        if not ref:
            raise ValueError("departamento scope requires ref (name)")
        geom = await _resolve_department(session, ref)
    elif scope == "seccion":
        if not ref:
            raise ValueError("seccion scope requires ref (unit_id)")
        geom = await _resolve_seccion(session, ref)
    else:
        raise ValueError(f"Unsupported scope: {scope}")

    _SCOPE_CACHE[cache_key] = geom
    return geom


# ── Clipping raster del PNG ────────────────────────────────────────────

def clip_png_tile_to_aoi(
    png_bytes: bytes, z: int, x: int, y: int, geom: BaseGeometry
) -> bytes:
    """
    Multiplica el canal alpha del PNG por una máscara binaria (256x256) donde
    la geometría cae dentro del tile. Lo que queda fuera pasa a transparente.

    Usa Pillow (sin rasterio) para mantener dependencias mínimas: computamos
    el bbox del tile en lng/lat, rasterizamos cada píxel preguntando si su
    centro (en lng/lat) está dentro del polígono. Para tiles 256x256 = 65k
    puntos, el check con prepared geom toma ~30-80ms, aceptable para tiles
    parciales (la mayoría de tiles son full-inside o full-outside y no llegan
    a esta función).
    """
    lng_min, lat_min, lng_max, lat_max = tile_bbox(z, x, y)
    width = height = 256

    # Mesh de coordenadas pixel center
    px_lng = lng_min + (np.arange(width) + 0.5) / width * (lng_max - lng_min)
    # Nota: lat se proyecta Web Mercator no-lineal, pero para z>=7 la distorsión
    # en un tile individual es < 1%, aceptable para masking.
    px_lat = lat_max - (np.arange(height) + 0.5) / height * (lat_max - lat_min)
    lng_grid, lat_grid = np.meshgrid(px_lng, px_lat)

    # Check en chunks para no saturar memoria
    mask = np.zeros((height, width), dtype=bool)
    prepared = _as_prepared(geom)
    from shapely.geometry import Point
    # Vectorizar con contains en batch sería ideal, pero shapely no lo tiene
    # directo. Fallback: loop por row (aún rápido con prepared).
    for row in range(height):
        for col in range(width):
            if prepared.contains(Point(lng_grid[row, col], lat_grid[row, col])):
                mask[row, col] = True

    # Abrir PNG y multiplicar alpha por la máscara
    img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    if img.size != (width, height):
        img = img.resize((width, height), Image.BILINEAR)
    arr = np.array(img)
    # arr shape: (256, 256, 4) RGBA
    arr[..., 3] = (arr[..., 3] * mask).astype(np.uint8)
    out = Image.fromarray(arr, mode="RGBA")
    buf = io.BytesIO()
    out.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def invalidate_country_cache() -> None:
    """Usar solo si cambian los polígonos departamentales (manual)."""
    global _COUNTRY_GEOM
    _COUNTRY_GEOM = None
    _SCOPE_CACHE.pop(("nacional", None), None)


def stats() -> dict[str, int]:
    """Para /api/v1/diagnostics."""
    return {
        "country_cached": _COUNTRY_GEOM is not None,
        "scope_cache_size": len(_SCOPE_CACHE),
        "prepared_cache_size": len(_PREPARED_CACHE),
    }
