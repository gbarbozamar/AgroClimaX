"""
Tests unitarios del módulo aoi_tile_clip.

Probamos lo que es pura lógica geométrica (sin DB):
  - tile_bbox en distintos zoom
  - tile_intersects con geometrías sintéticas
  - tile_fully_contained
  - clip_png_tile_to_aoi que efectivamente multiplica alpha

Los resolvers que consultan DB (resolve_scope_geometry) están cubiertos
por tests de integración separados (test_clip_endpoints.py cuando exista).
"""
from __future__ import annotations

import io

import numpy as np
import pytest
from PIL import Image
from shapely.geometry import Polygon, box

from app.services import aoi_tile_clip


def _make_png(color=(200, 50, 50, 255), size=256) -> bytes:
    arr = np.zeros((size, size, 4), dtype=np.uint8)
    arr[..., :] = color
    img = Image.fromarray(arr, mode="RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def test_tile_bbox_z0():
    bbox = aoi_tile_clip.tile_bbox(0, 0, 0)
    assert bbox[0] == pytest.approx(-180.0)
    assert bbox[2] == pytest.approx(180.0)
    # Web Mercator lat limit ≈ ±85.05°
    assert bbox[1] == pytest.approx(-85.05, abs=0.1)
    assert bbox[3] == pytest.approx(85.05, abs=0.1)


def test_tile_bbox_z7_rivera():
    """Rivera city ~(-30.9, -55.55) cae en algún tile z=7 de la columna x=44."""
    # Verificamos que existe un tile en x=44 que contiene a Rivera.
    for y_candidate in range(70, 80):
        bbox = aoi_tile_clip.tile_bbox(7, 44, y_candidate)
        if bbox[0] <= -55.55 <= bbox[2] and bbox[1] <= -30.9 <= bbox[3]:
            return  # found
    pytest.fail("No tile at z=7,x=44 contains Rivera (-30.9, -55.55)")


def test_tile_intersects_none_geom():
    # Sin scope = todos los tiles pasan
    assert aoi_tile_clip.tile_intersects(10, 500, 300, None) is True


def test_tile_intersects_full_uruguay_box():
    # Polígono que cubre todo Uruguay
    uy = box(-58.5, -35.0, -53.0, -30.0)
    # Tile z=7 dentro de Uruguay
    assert aoi_tile_clip.tile_intersects(7, 44, 76, uy) is True
    # Tile z=7 sobre océano Atlántico al sur (lng ~-50, lat ~-45)
    # z=7, n=128. lng=-50 -> x = (180-50)/360*128 ≈ 46
    # lat=-45 -> más al sur del límite Uruguay
    assert aoi_tile_clip.tile_intersects(7, 46, 80, uy) is False


def test_tile_fully_contained():
    big = box(-180, -85, 180, 85)  # mundo entero
    small = box(-55.5, -31.0, -55.0, -30.5)  # pedazo dentro de Uruguay
    # Un tile z=10 cerca de Rivera debería estar contenido en el mundo
    assert aoi_tile_clip.tile_fully_contained(10, 350, 600, big) is True
    # Un tile z=5 global NO está contenido en un polígono chico
    assert aoi_tile_clip.tile_fully_contained(5, 10, 15, small) is False


def test_clip_png_full_inside():
    """Si el polígono cubre todo el tile, el alpha no debería cambiar."""
    png = _make_png(color=(100, 200, 100, 255))
    # Tile z=10 en Uruguay, geom = mundo entero
    world = box(-180, -85, 180, 85)
    clipped = aoi_tile_clip.clip_png_tile_to_aoi(png, 10, 350, 600, world)
    img = Image.open(io.BytesIO(clipped)).convert("RGBA")
    arr = np.array(img)
    # Al menos 95% de pixeles deben tener alpha 255 (el resto puede ser bordes)
    alpha_pct = (arr[..., 3] == 255).sum() / arr[..., 3].size
    assert alpha_pct > 0.95, f"expected alpha mostly 255, got {alpha_pct:.2%}"


def test_clip_png_full_outside():
    """Si la geom no toca el tile, el alpha debe quedar en 0."""
    png = _make_png()
    # Polígono sobre Europa
    europe = box(0, 40, 10, 50)
    clipped = aoi_tile_clip.clip_png_tile_to_aoi(png, 10, 350, 600, europe)
    img = Image.open(io.BytesIO(clipped)).convert("RGBA")
    arr = np.array(img)
    assert arr[..., 3].max() == 0, "alpha debe ser 0 si geom no toca el tile"


def test_invalidate_country_cache():
    """El invalidate debe resetear el flag interno."""
    aoi_tile_clip._COUNTRY_GEOM = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    aoi_tile_clip._SCOPE_CACHE[("nacional", None)] = aoi_tile_clip._COUNTRY_GEOM
    aoi_tile_clip.invalidate_country_cache()
    assert aoi_tile_clip._COUNTRY_GEOM is None
    assert ("nacional", None) not in aoi_tile_clip._SCOPE_CACHE


def test_stats_shape():
    s = aoi_tile_clip.stats()
    assert "country_cached" in s
    assert "scope_cache_size" in s
    assert "prepared_cache_size" in s
