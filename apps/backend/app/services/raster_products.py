from __future__ import annotations

import asyncio
import hashlib
import io
import math
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from PIL import Image
import numpy as np
from morecantile import Tile
from morecantile.defaults import tms
_RASTERIO_PROJ_DIR = Path(__file__).resolve().parents[2] / ".venv" / "Lib" / "site-packages" / "rasterio" / "proj_data"
if _RASTERIO_PROJ_DIR.exists():
    os.environ["PROJ_LIB"] = str(_RASTERIO_PROJ_DIR)
    os.environ["PROJ_DATA"] = str(_RASTERIO_PROJ_DIR)

import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
from rio_tiler.io import Reader
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from shapely.geometry import shape

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.humedad import AOIUnit
from app.models.materialized import RasterMosaic, RasterProduct
from app.services.object_storage import storage_get_bytes, storage_put_bytes
from app.services.raster_cache import parse_bbox_values


RASTER_PRODUCTS_DIR = Path(__file__).resolve().parents[2] / ".raster_products"
RASTER_PRODUCTS_DIR.mkdir(exist_ok=True)
TILE_CACHE_DIR = Path(__file__).resolve().parents[2] / ".tile_cache"
TILE_SIZE = 256
WEB_MERCATOR_TMS = tms.get("WebMercatorQuad")
DEPARTMENT_CARRY_FORWARD_LAYERS = {"alerta_fusion", "rgb", "ndvi", "ndmi", "ndwi", "savi"}
DEPARTMENT_CARRY_FORWARD_LOOKBACK_DAYS = 21
NEAR_REAL_TIME_CARRY_FORWARD_LAYERS = {"alerta_fusion"}


def _build_transparent_tile_png() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0)).save(buffer, format="PNG")
    return buffer.getvalue()


TRANSPARENT_TILE_PNG = _build_transparent_tile_png()


def _visual_empty_product_threshold_pct(layer_id: str) -> float:
    normalized = str(layer_id or "").strip().lower()
    if normalized == "alerta_fusion":
        return 2.0
    if normalized == "rgb":
        return 8.0
    if normalized in {"ndvi", "ndmi", "ndwi", "savi"}:
        return 10.0
    if normalized in {"sar", "lst"}:
        return 6.0
    return 10.0


def _cloudlike_pixel_mask_from_rgba(array_rgba: np.ndarray) -> np.ndarray:
    if array_rgba.ndim != 3 or array_rgba.shape[2] < 4:
        return np.zeros(array_rgba.shape[:2], dtype=bool)
    alpha = array_rgba[:, :, 3] > 0
    rgb = array_rgba[:, :, :3].astype(np.int16, copy=False)
    bright = (rgb[:, :, 0] > 215) & (rgb[:, :, 1] > 215) & (rgb[:, :, 2] > 215)
    low_chroma = (rgb.max(axis=2) - rgb.min(axis=2)) < 24
    return alpha & bright & low_chroma


def _cloudlike_pct_from_rgba(array_rgba: np.ndarray) -> float:
    if array_rgba.ndim != 3 or array_rgba.shape[2] < 4:
        return 0.0
    alpha = array_rgba[:, :, 3] > 0
    total = int(alpha.sum())
    if total <= 0:
        return 100.0
    cloudlike = _cloudlike_pixel_mask_from_rgba(array_rgba)
    return round((float(cloudlike.sum()) / float(total)) * 100.0, 2)


def _max_cloudlike_tile_pct_from_rgba(array_rgba: np.ndarray, *, block_size: int = TILE_SIZE) -> float:
    if array_rgba.ndim != 3 or array_rgba.shape[2] < 4:
        return 0.0
    alpha = array_rgba[:, :, 3] > 0
    cloudlike = _cloudlike_pixel_mask_from_rgba(array_rgba)
    max_pct = 0.0
    height, width = cloudlike.shape
    for top in range(0, height, block_size):
        for left in range(0, width, block_size):
            block_alpha = alpha[top:top + block_size, left:left + block_size]
            total = int(block_alpha.sum())
            if total <= 0:
                continue
            block_cloudlike = cloudlike[top:top + block_size, left:left + block_size]
            pct = (float(block_cloudlike.sum()) / float(total)) * 100.0
            if pct > max_pct:
                max_pct = pct
    return round(max_pct, 2)


def _max_cloudlike_tile_threshold_pct(layer_id: str) -> float:
    normalized = str(layer_id or "").strip().lower()
    if normalized == "rgb":
        return 35.0
    return 100.0


def _metadata_rgb_cloud_degraded(metadata: dict[str, Any] | None) -> bool:
    metadata = metadata or {}
    normalized_layer = str(metadata.get("layer_id") or "").strip().lower()
    cloudlike_max_tile_pct = float(metadata.get("cloudlike_max_tile_pct") or 0.0)
    cloudlike_pct = float(metadata.get("cloudlike_pct") or 0.0)
    if normalized_layer == "rgb":
        return cloudlike_max_tile_pct >= _max_cloudlike_tile_threshold_pct("rgb") or cloudlike_pct >= 12.0
    return False


def _stored_product_visible_pct(metadata: dict[str, Any] | None) -> float:
    metadata = metadata or {}
    for key in ("visible_pixel_pct", "renderable_pixel_pct", "valid_pixel_pct"):
        try:
            value = metadata.get(key)
            if value is None:
                continue
            return max(0.0, float(value))
        except Exception:
            continue
    return 0.0


def _product_has_usable_pixels(layer_id: str, metadata: dict[str, Any] | None) -> bool:
    visible_pct = _stored_product_visible_pct(metadata)
    threshold_pct = _visual_empty_product_threshold_pct(layer_id)
    return visible_pct >= threshold_pct


def _effective_row_visual_empty(layer_id: str, row_visual_empty: bool, metadata: dict[str, Any] | None) -> bool:
    metadata = metadata or {}
    if _product_has_usable_pixels(layer_id, metadata):
        return False
    if str(metadata.get("visual_state") or "").strip().lower() in {"ready", "interpolated"} and _stored_product_visible_pct(metadata) > 0.0:
        return False
    return bool(row_visual_empty)


def _native_resolution_m(layer_id: str) -> int:
    normalized = str(layer_id or "").strip().lower()
    if normalized == "lst":
        return 1000
    return 10


def _base_raster_build_metadata(layer_id: str) -> dict[str, Any]:
    return {
        "build_version": str(getattr(settings, "raster_product_build_version", "raster-v1")),
        "visual_style_version": str(getattr(settings, "raster_visual_style_version", "visual-v1")),
        "native_resolution_m": _native_resolution_m(layer_id),
    }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _date_to_datetime(value: date | str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value)
        except Exception:
            return None
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _geometry_object(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return None
    if isinstance(value, dict):
        return value
    return None


def raster_product_key(
    *,
    layer_id: str,
    display_date: date | str,
    zoom: int,
    bbox_bucket: str,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    product_kind: str = "viewport_bucket_mosaic",
) -> str:
    return "::".join(
        [
            product_kind,
            layer_id,
            str(display_date),
            str(zoom),
            str(bbox_bucket or "auto"),
            str(scope_type or "-"),
            str(scope_ref or "-"),
        ]
    )


def raster_product_storage_key(
    *,
    layer_id: str,
    display_date: date | str,
    zoom: int,
    bbox_bucket: str,
) -> str:
    safe_bucket = hashlib.sha256(str(bbox_bucket or "auto").encode("utf-8")).hexdigest()[:24]
    return f"raster-products/{layer_id}/{display_date}/{zoom}/{safe_bucket}.png"


def _product_fs_path(storage_key: str) -> Path:
    path = RASTER_PRODUCTS_DIR.joinpath(*PurePosixPath(storage_key).parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _tile_bucket_key(layer_id: str, z: int, x: int, y: int, *, target_date: date | None = None) -> str:
    target_date = target_date or date.today()
    return f"tiles/{target_date.isoformat()}/{layer_id}/{z}/{x}/{y}.png"


def _lon_to_tile_x(lon: float, zoom: int) -> int:
    n = 2**zoom
    lon = max(-180.0, min(180.0, lon))
    return max(0, min(n - 1, int(math.floor((lon + 180.0) / 360.0 * n))))


def _lat_to_tile_y(lat: float, zoom: int) -> int:
    n = 2**zoom
    lat = max(-85.05112878, min(85.05112878, lat))
    lat_rad = math.radians(lat)
    tile_y = int(math.floor((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n))
    return max(0, min(n - 1, tile_y))


def _parse_bbox_string(bbox: str | list[float] | tuple[float, ...] | None) -> tuple[float, float, float, float] | None:
    if bbox is None:
        return None
    if isinstance(bbox, str):
        parts = [part.strip() for part in bbox.split(",")[:4]]
    elif isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
        parts = [str(part).strip() for part in bbox[:4]]
    else:
        return None
    if len(parts) < 4:
        return None
    try:
        west, south, east, north = [float(part) for part in parts]
    except Exception:
        return None
    return west, south, east, north


def _asset_intersects_tile(asset: dict[str, Any], *, x: int, y: int, z: int) -> bool:
    bbox = _parse_bbox_string(asset.get("bbox"))
    if bbox is not None:
        west, south, east, north = bbox
        min_x = _lon_to_tile_x(west, z)
        max_x = _lon_to_tile_x(east, z)
        min_y = _lat_to_tile_y(north, z)
        max_y = _lat_to_tile_y(south, z)
        return min_x <= x <= max_x and min_y <= y <= max_y
    asset_zoom = asset.get("zoom")
    try:
        if asset_zoom is not None and int(asset_zoom) == int(z):
            min_x = int(asset.get("tile_min_x"))
            max_x = int(asset.get("tile_max_x"))
            min_y = int(asset.get("tile_min_y"))
            max_y = int(asset.get("tile_max_y"))
            return min_x <= x <= max_x and min_y <= y <= max_y
    except Exception:
        return True
    return True


def _tile_coords_for_bbox(bbox: str | tuple[float, float, float, float] | list[float] | None, zoom: int) -> list[tuple[int, int]]:
    resolved = parse_bbox_values(bbox)
    if resolved is None:
        return []
    west, south, east, north = resolved
    x_min = _lon_to_tile_x(west, zoom)
    x_max = _lon_to_tile_x(east, zoom)
    y_min = _lat_to_tile_y(north, zoom)
    y_max = _lat_to_tile_y(south, zoom)
    return [
        (x, y)
        for x in range(min(x_min, x_max), max(x_min, x_max) + 1)
        for y in range(min(y_min, y_max), max(y_min, y_max) + 1)
    ]


async def _load_tile_bytes(*, layer_id: str, source_date: date, zoom: int, x: int, y: int) -> bytes | None:
    cache_path = TILE_CACHE_DIR / f"{layer_id}_{source_date.isoformat()}_{zoom}_{x}_{y}.png"
    if cache_path.exists():
        try:
            return cache_path.read_bytes()
        except Exception:
            return None
    bucket_cached = await storage_get_bytes(_tile_bucket_key(layer_id, zoom, x, y, target_date=source_date))
    if bucket_cached:
        content = bucket_cached[0]
        try:
            cache_path.write_bytes(content)
        except Exception:
            pass
        return content
    return None


def _visible_pixel_pct(content: bytes) -> float:
    try:
        with Image.open(io.BytesIO(content)) as image:
            rgba = image.convert("RGBA")
            alpha = rgba.getchannel("A")
            histogram = alpha.histogram()
            total_pixels = max(rgba.size[0] * rgba.size[1], 1)
            transparent_pixels = int(histogram[0] if histogram else 0)
            return round(((total_pixels - transparent_pixels) / total_pixels) * 100.0, 2)
    except Exception:
        return 0.0


async def upsert_raster_product(
    session: AsyncSession,
    *,
    product_key: str,
    layer_id: str,
    product_kind: str,
    scope_type: str | None,
    scope_ref: str | None,
    display_date: date | str,
    source_date: date | str | None,
    zoom: int,
    bbox_bucket: str,
    storage_backend: str,
    storage_key: str,
    content_type: str,
    width: int,
    height: int,
    tile_min_x: int,
    tile_min_y: int,
    tile_max_x: int,
    tile_max_y: int,
    visual_empty: bool,
    status: str,
    bytes_size: int | None,
    metadata_extra: dict[str, Any] | None = None,
) -> RasterProduct:
    result = await session.execute(select(RasterProduct).where(RasterProduct.product_key == product_key).limit(1))
    row = result.scalar_one_or_none()
    if row is None:
        row = RasterProduct(product_key=product_key, layer_id=layer_id, product_kind=product_kind)
        session.add(row)
    row.scope_type = scope_type
    row.scope_ref = scope_ref
    row.display_date = _date_to_datetime(display_date)
    row.source_date = _date_to_datetime(source_date)
    row.zoom = zoom
    row.bbox_bucket = bbox_bucket
    row.storage_backend = storage_backend
    row.storage_key = storage_key
    row.content_type = content_type
    row.width = width
    row.height = height
    row.tile_min_x = tile_min_x
    row.tile_min_y = tile_min_y
    row.tile_max_x = tile_max_x
    row.tile_max_y = tile_max_y
    row.visual_empty = 1 if visual_empty else 0
    row.status = status
    row.bytes_size = bytes_size
    row.metadata_extra = metadata_extra or {}
    row.updated_at = _now_utc()
    await session.flush()
    return row


async def get_ready_raster_product(
    *,
    layer_id: str,
    display_date: date | str,
    zoom: int,
    bbox_bucket: str,
    scope_type: str | None,
    scope_ref: str | None,
    product_kind: str = "viewport_bucket_mosaic",
) -> RasterProduct | None:
    try:
        async with AsyncSessionLocal() as session:
            query = select(RasterProduct).where(
                RasterProduct.product_key
                == raster_product_key(
                    layer_id=layer_id,
                    display_date=display_date,
                    zoom=zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=scope_ref,
                    product_kind=product_kind,
                )
            ).limit(1)
            result = await session.execute(query)
            row = result.scalar_one_or_none()
    except Exception:
        return None
    if row is None or row.status not in {"ready", "empty"}:
        return None
    return row


async def materialize_viewport_raster_product(
    *,
    layer_id: str,
    display_date: date,
    source_date: date | str | None,
    bbox: str | None,
    zoom: int,
    bbox_bucket: str,
    scope_type: str | None,
    scope_ref: str | None,
    metadata_extra: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    tile_coords = _tile_coords_for_bbox(bbox, zoom)
    if not tile_coords:
        return None

    resolved_source_date = source_date
    if isinstance(resolved_source_date, str):
        try:
            resolved_source_date = date.fromisoformat(resolved_source_date)
        except Exception:
            resolved_source_date = display_date
    if resolved_source_date is None:
        resolved_source_date = display_date

    xs = sorted({x for x, _ in tile_coords})
    ys = sorted({y for _, y in tile_coords})
    min_x, max_x = xs[0], xs[-1]
    min_y, max_y = ys[0], ys[-1]
    width = (max_x - min_x + 1) * TILE_SIZE
    height = (max_y - min_y + 1) * TILE_SIZE
    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    had_any_tile = False

    for tile_x, tile_y in tile_coords:
        content = await _load_tile_bytes(
            layer_id=layer_id,
            source_date=resolved_source_date,
            zoom=zoom,
            x=tile_x,
            y=tile_y,
        )
        if not content:
            continue
        try:
            with Image.open(io.BytesIO(content)) as image:
                rgba = image.convert("RGBA")
                canvas.paste(rgba, ((tile_x - min_x) * TILE_SIZE, (tile_y - min_y) * TILE_SIZE))
                had_any_tile = True
        except Exception:
            continue

    if not had_any_tile:
        return None

    buffer = io.BytesIO()
    canvas.save(buffer, format="PNG")
    content = buffer.getvalue()
    visible_pct = _visible_pixel_pct(content)
    threshold_pct = _visual_empty_product_threshold_pct(layer_id)
    visual_empty = visible_pct < threshold_pct
    product_key = raster_product_key(
        layer_id=layer_id,
        display_date=display_date,
        zoom=zoom,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    storage_key = raster_product_storage_key(
        layer_id=layer_id,
        display_date=display_date,
        zoom=zoom,
        bbox_bucket=bbox_bucket,
    )
    fs_path = _product_fs_path(storage_key)
    try:
        fs_path.write_bytes(content)
    except Exception:
        pass
    await storage_put_bytes(storage_key, content, content_type="image/png")

    merged_metadata = {
        **(metadata_extra or {}),
        "bbox": bbox,
        "bbox_bucket": bbox_bucket,
        "visible_pixel_pct": visible_pct,
        "visual_empty_threshold_pct": threshold_pct,
        "tile_min_x": min_x,
        "tile_min_y": min_y,
        "tile_max_x": max_x,
        "tile_max_y": max_y,
        "source_date": resolved_source_date.isoformat(),
    }
    try:
        async with AsyncSessionLocal() as session:
            await upsert_raster_product(
                session,
                product_key=product_key,
                layer_id=layer_id,
                product_kind="viewport_bucket_mosaic",
                scope_type=scope_type,
                scope_ref=scope_ref,
                display_date=display_date,
                source_date=resolved_source_date,
                zoom=zoom,
                bbox_bucket=bbox_bucket,
                storage_backend="filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem",
                storage_key=storage_key,
                content_type="image/png",
                width=width,
                height=height,
                tile_min_x=min_x,
                tile_min_y=min_y,
                tile_max_x=max_x,
                tile_max_y=max_y,
                visual_empty=visual_empty,
                status="empty" if visual_empty else "ready",
                bytes_size=len(content),
                metadata_extra=merged_metadata,
            )
            await session.commit()
    except Exception:
        pass
    return {
        "product_key": product_key,
        "storage_key": storage_key,
        "visual_empty": visual_empty,
        "visible_pixel_pct": visible_pct,
        "tile_min_x": min_x,
        "tile_min_y": min_y,
        "tile_max_x": max_x,
        "tile_max_y": max_y,
    }


async def read_viewport_raster_product_tile(
    *,
    layer_id: str,
    display_date: date | str,
    zoom: int,
    bbox_bucket: str,
    scope_type: str | None,
    scope_ref: str | None,
    x: int,
    y: int,
) -> tuple[bytes | None, dict[str, Any] | None]:
    row = await get_ready_raster_product(
        layer_id=layer_id,
        display_date=display_date,
        zoom=zoom,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    if row is None:
        return None, None

    metadata = dict(row.metadata_extra or {})
    effective_visual_empty = _effective_row_visual_empty(layer_id, bool(row.visual_empty), metadata)
    visible_pct = _stored_product_visible_pct(metadata)
    if effective_visual_empty and visible_pct <= 0.0:
        return None, metadata
    if row.tile_min_x is None or row.tile_min_y is None or row.tile_max_x is None or row.tile_max_y is None:
        return None, None
    if x < row.tile_min_x or x > row.tile_max_x or y < row.tile_min_y or y > row.tile_max_y:
        return None, None

    fs_path = _product_fs_path(row.storage_key or "")
    content: bytes | None = None
    if row.storage_key and fs_path.exists():
        try:
            content = fs_path.read_bytes()
        except Exception:
            content = None
    if content is None and row.storage_key:
        bucket_cached = await storage_get_bytes(row.storage_key)
        if bucket_cached:
            content = bucket_cached[0]
            try:
                fs_path.parent.mkdir(parents=True, exist_ok=True)
                fs_path.write_bytes(content)
            except Exception:
                pass
    if content is None:
        return None, None

    left = (x - row.tile_min_x) * TILE_SIZE
    top = (y - row.tile_min_y) * TILE_SIZE
    try:
        with Image.open(io.BytesIO(content)) as image:
            rgba = image.convert("RGBA")
            tile = rgba.crop((left, top, left + TILE_SIZE, top + TILE_SIZE))
            tile_buffer = io.BytesIO()
            tile.save(tile_buffer, format="PNG")
            metadata.update(
                {
                    "layer_id": layer_id,
                    "renderable_pixel_pct": max(visible_pct, float(metadata.get("renderable_pixel_pct") or 0.0)),
                    "visual_empty": effective_visual_empty and visible_pct <= 0.0,
                    "visual_state": "empty" if effective_visual_empty and visible_pct <= 0.0 else str(metadata.get("visual_state") or row.status or "ready"),
                    "cache_status": "ready" if str(row.status or "").strip().lower() == "ready" else row.status,
                    "coverage_origin": metadata.get("coverage_origin") or row.product_kind,
                }
            )
            return tile_buffer.getvalue(), metadata
    except Exception:
        return None, None


async def _load_raster_product_content(row: RasterProduct) -> bytes | None:
    fs_path = _product_fs_path(row.storage_key or "")
    if row.storage_key and fs_path.exists():
        try:
            return fs_path.read_bytes()
        except Exception:
            pass
    if row.storage_key:
        bucket_cached = await storage_get_bytes(row.storage_key)
        if bucket_cached:
            content = bucket_cached[0]
            try:
                fs_path.parent.mkdir(parents=True, exist_ok=True)
                fs_path.write_bytes(content)
            except Exception:
                pass
            return content
    return None


def _product_tile_bounds(row: RasterProduct) -> tuple[float, float, float, float] | None:
    if row.tile_min_x is None or row.tile_min_y is None or row.tile_max_x is None or row.tile_max_y is None:
        return None
    left_bounds = _tile_xy_bounds(int(row.tile_min_x), int(row.tile_min_y), int(row.zoom or 0))
    right_bounds = _tile_xy_bounds(int(row.tile_max_x), int(row.tile_max_y), int(row.zoom or 0))
    return (
        float(left_bounds[0]),
        float(right_bounds[1]),
        float(right_bounds[2]),
        float(left_bounds[3]),
    )


def _bounds_intersect(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> bool:
    return not (a[2] <= b[0] or a[0] >= b[2] or a[3] <= b[1] or a[1] >= b[3])


def _crop_tile_from_viewport_mosaic(
    *,
    content: bytes,
    row: RasterProduct,
    x: int,
    y: int,
    z: int,
) -> bytes | None:
    row_bounds = _product_tile_bounds(row)
    if row_bounds is None:
        return None
    req_bounds = _tile_xy_bounds(x, y, z)
    if not _bounds_intersect(req_bounds, row_bounds):
        return None
    src_left, src_bottom, src_right, src_top = row_bounds
    req_left, req_bottom, req_right, req_top = req_bounds
    try:
        with Image.open(io.BytesIO(content)) as image:
            rgba = image.convert("RGBA")
            width, height = rgba.size
            x_scale = width / max(src_right - src_left, 1.0)
            y_scale = height / max(src_top - src_bottom, 1.0)
            crop_left = max(0, min(width, int(math.floor((req_left - src_left) * x_scale))))
            crop_right = max(0, min(width, int(math.ceil((req_right - src_left) * x_scale))))
            crop_top = max(0, min(height, int(math.floor((src_top - req_top) * y_scale))))
            crop_bottom = max(0, min(height, int(math.ceil((src_top - req_bottom) * y_scale))))
            if crop_right <= crop_left or crop_bottom <= crop_top:
                return None
            tile = rgba.crop((crop_left, crop_top, crop_right, crop_bottom))
            if tile.size != (TILE_SIZE, TILE_SIZE):
                tile = tile.resize((TILE_SIZE, TILE_SIZE), resample=Image.BILINEAR)
            if int(np.count_nonzero(np.array(tile)[:, :, 3] > 0)) <= 0:
                return None
            buffer = io.BytesIO()
            tile.save(buffer, format="PNG")
            return buffer.getvalue()
    except Exception:
        return None


async def read_scope_viewport_raster_fallback_tile(
    *,
    layer_id: str,
    display_date: date,
    scope_type: str,
    scope_ref: str,
    x: int,
    y: int,
    z: int,
) -> tuple[bytes | None, dict[str, Any] | None]:
    target_dt = _date_to_datetime(display_date)
    if target_dt is None:
        return None, None
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(RasterProduct).where(
                    RasterProduct.layer_id == layer_id,
                    RasterProduct.product_kind == "viewport_bucket_mosaic",
                    RasterProduct.scope_type == scope_type,
                    RasterProduct.scope_ref == scope_ref,
                    RasterProduct.display_date == target_dt,
                    RasterProduct.status.in_(["ready", "empty"]),
                ).order_by(RasterProduct.zoom.desc(), RasterProduct.updated_at.desc()).limit(32)
            )
            rows = list(result.scalars().all())
    except Exception:
        return None, None
    if not rows:
        return None, None
    req_bounds = _tile_xy_bounds(x, y, z)
    for row in rows:
        metadata = dict(row.metadata_extra or {})
        visible_pct = _stored_product_visible_pct(metadata)
        effective_visual_empty = _effective_row_visual_empty(layer_id, bool(row.visual_empty), metadata)
        if effective_visual_empty and visible_pct <= 0.0:
            continue
        row_bounds = _product_tile_bounds(row)
        if row_bounds is None or not _bounds_intersect(req_bounds, row_bounds):
            continue
        content = await _load_raster_product_content(row)
        if not content:
            continue
        tile_bytes = _crop_tile_from_viewport_mosaic(content=content, row=row, x=x, y=y, z=z)
        if not tile_bytes:
            continue
        metadata.update(
            {
                "layer_id": layer_id,
                "renderable_pixel_pct": max(visible_pct, float(metadata.get("renderable_pixel_pct") or 0.0)),
                "visual_empty": False,
                "visual_state": str(metadata.get("visual_state") or "ready"),
                "cache_status": "ready",
                "coverage_origin": "viewport_bucket_mosaic_fallback",
            }
        )
        return tile_bytes, metadata
    return None, None


def canonical_zoom_for_layer(layer_id: str) -> int:
    normalized = str(layer_id or "").strip().lower()
    if normalized == "lst":
        return max(7, min(17, int(settings.raster_canonical_zoom_lst)))
    if normalized == "sar":
        return max(7, min(17, int(settings.raster_canonical_zoom_sar)))
    if normalized == "alerta_fusion":
        return max(7, min(17, int(settings.raster_canonical_zoom_alerta)))
    return max(7, min(17, int(settings.raster_canonical_zoom_optical)))


def _canonical_bbox_bucket(scope_type: str, scope_ref: str, zoom: int) -> str:
    normalized_scope = str(scope_type or "global").strip().lower() or "global"
    normalized_ref = str(scope_ref or "default").strip() or "default"
    safe_ref = hashlib.sha256(normalized_ref.encode("utf-8")).hexdigest()[:20]
    return f"{normalized_scope}:{safe_ref}:z{int(zoom)}"


def department_raster_product_key(*, layer_id: str, display_date: date | str, department: str, zoom: int) -> str:
    return raster_product_key(
        layer_id=layer_id,
        display_date=display_date,
        zoom=zoom,
        bbox_bucket=_canonical_bbox_bucket("departamento", department, zoom),
        scope_type="departamento",
        scope_ref=department,
        product_kind="department_daily_cog",
    )


def department_raster_storage_key(*, layer_id: str, display_date: date | str, department: str) -> str:
    safe_department = hashlib.sha256(str(department).encode("utf-8")).hexdigest()[:20]
    build_version = str(settings.raster_product_build_version or "raster-v1")
    return f"raster-products/cogs/{build_version}/{layer_id}/{display_date}/{safe_department}.tif"


def national_mosaic_key(*, layer_id: str, display_date: date | str) -> str:
    return "::".join(["national_mosaic", layer_id, str(display_date), "Uruguay"])


def national_mosaic_storage_key(*, layer_id: str, display_date: date | str) -> str:
    build_version = str(settings.raster_product_build_version or "raster-v1")
    return f"raster-products/mosaics/{build_version}/{layer_id}/{display_date}/national_mosaic.json"


def _renderable_pct_from_rgba(array_rgba: np.ndarray) -> float:
    if array_rgba.size == 0:
        return 0.0
    alpha = array_rgba[:, :, 3]
    total_pixels = max(alpha.shape[0] * alpha.shape[1], 1)
    opaque_pixels = int(np.count_nonzero(alpha > 0))
    return round((opaque_pixels / total_pixels) * 100.0, 2)


def _tile_xy_bounds(tile_x: int, tile_y: int, tile_z: int) -> tuple[float, float, float, float]:
    bounds = WEB_MERCATOR_TMS.xy_bounds(Tile(x=tile_x, y=tile_y, z=tile_z))
    return float(bounds.left), float(bounds.bottom), float(bounds.right), float(bounds.top)


def _department_product_metadata(row: RasterProduct) -> dict[str, Any]:
    metadata = dict(row.metadata_extra or {})
    renderable_pct = _stored_product_visible_pct(metadata)
    visual_empty = _effective_row_visual_empty(str(row.layer_id or ""), bool(row.visual_empty), metadata)
    visual_state = "empty" if visual_empty or row.status == "empty" else ("ready" if row.status == "ready" else row.status)
    metadata.update(
        {
            **_base_raster_build_metadata(str(row.layer_id or "")),
            "layer_id": row.layer_id,
            "display_date": row.display_date.date().isoformat() if row.display_date else None,
            "source_date": row.source_date.date().isoformat() if row.source_date else None,
            "renderable_pixel_pct": renderable_pct,
            "visual_empty": visual_empty,
            "visual_state": visual_state,
            "coverage_origin": row.product_kind,
            "resolved_source_date": metadata.get("resolved_source_date") or (row.source_date.date().isoformat() if row.source_date else None),
            "cache_status": "ready" if row.status == "ready" else row.status,
        }
    )
    return metadata


def _mosaic_metadata(row: RasterMosaic) -> dict[str, Any]:
    metadata = dict(row.metadata_extra or {})
    assets = list(metadata.get("assets") or [])
    if "asset_count" not in metadata:
        metadata["asset_count"] = len(assets)
    if "usable_asset_count" not in metadata:
        metadata["usable_asset_count"] = int(metadata.get("asset_count") or 0)
    visual_empty = bool(row.visual_empty) or row.status in {"empty", "missing"}
    metadata.update(
        {
            **_base_raster_build_metadata(str(row.layer_id or "")),
            "layer_id": row.layer_id,
            "display_date": row.display_date.date().isoformat() if row.display_date else None,
            "visual_empty": visual_empty,
            "visual_state": "empty" if visual_empty else ("ready" if row.status == "ready" else row.status),
            "coverage_origin": "national_mosaic",
            "cache_status": "ready" if row.status == "ready" else row.status,
        }
    )
    return metadata


def _department_product_is_usable_for_mosaic(row: RasterProduct) -> bool:
    if row is None:
        return False
    if str(row.status or "").strip().lower() != "ready":
        return False
    metadata = dict(row.metadata_extra or {})
    if _effective_row_visual_empty(str(row.layer_id or ""), bool(row.visual_empty), metadata):
        return False
    if not _product_has_usable_pixels(str(row.layer_id or ""), metadata):
        return False
    return bool(str(row.storage_key or "").strip())


def _department_asset_entry(row: RasterProduct) -> dict[str, Any]:
    metadata = dict(row.metadata_extra or {})
    return {
        "department": str(row.scope_ref or ""),
        "product_key": str(row.product_key or ""),
        "storage_key": str(row.storage_key or ""),
        "bbox": metadata.get("bbox"),
        "zoom": int(row.zoom or 0),
        "tile_min_x": int(row.tile_min_x or 0),
        "tile_min_y": int(row.tile_min_y or 0),
        "tile_max_x": int(row.tile_max_x or 0),
        "tile_max_y": int(row.tile_max_y or 0),
        "renderable_pixel_pct": _stored_product_visible_pct(metadata),
        "resolved_source_date": metadata.get("resolved_source_date") or (row.source_date.date().isoformat() if row.source_date else None),
        "visual_empty": _effective_row_visual_empty(str(row.layer_id or ""), bool(row.visual_empty), metadata),
    }


_RUNTIME_NATIONAL_MOSAIC_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_RUNTIME_NATIONAL_MOSAIC_CACHE_TTL_SECONDS = 60.0
_RUNTIME_NATIONAL_MOSAIC_CACHE_LOCK = asyncio.Lock()


async def _get_runtime_national_mosaic_assets(
    *,
    layer_id: str,
    display_date: date,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cache_key = (str(layer_id or ""), display_date.isoformat())
    now_ts = _now_utc().timestamp()
    cached = _RUNTIME_NATIONAL_MOSAIC_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("ts") or 0.0)) <= _RUNTIME_NATIONAL_MOSAIC_CACHE_TTL_SECONDS:
        return list(cached.get("assets") or []), dict(cached.get("metadata") or {})

    target_dt = _date_to_datetime(display_date)
    rows: list[RasterProduct] = []
    if target_dt is not None:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(RasterProduct).where(
                        RasterProduct.layer_id == layer_id,
                        RasterProduct.product_kind == "department_daily_cog",
                        RasterProduct.scope_type == "departamento",
                        RasterProduct.display_date == target_dt,
                        RasterProduct.status.in_(["ready", "empty"]),
                    )
                )
                rows = list(result.scalars().all())
        except Exception:
            rows = []

    usable_rows = [row for row in rows if _department_product_is_usable_for_mosaic(row)]
    assets = [_department_asset_entry(row) for row in usable_rows]
    visual_empty = len(assets) <= 0
    metadata = {
        "layer_id": layer_id,
        "display_date": display_date.isoformat(),
        "scope_type": "nacional",
        "scope_ref": "Uruguay",
        "asset_count": len(assets),
        "usable_asset_count": len(assets),
        "department_asset_count": len(rows),
        "visual_empty": visual_empty,
        "visual_state": "empty" if visual_empty else "ready",
        "coverage_origin": "national_mosaic_runtime",
        "cache_status": "empty" if visual_empty else "ready",
    }

    async with _RUNTIME_NATIONAL_MOSAIC_CACHE_LOCK:
        _RUNTIME_NATIONAL_MOSAIC_CACHE[cache_key] = {
            "ts": now_ts,
            "assets": assets,
            "metadata": metadata,
        }
    return assets, metadata


async def _resolve_scope_department(
    *,
    unit_id: str | None = None,
    department: str | None = None,
) -> str | None:
    if department:
        return str(department)
    if not unit_id:
        return None
    async with AsyncSessionLocal() as session:
        row = await session.get(AOIUnit, unit_id)
    if row is not None:
        return str(row.department)
    return None


async def resolve_canonical_product_scope(
    *,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, str]:
    normalized_scope_type = str(scope_type or "").strip().lower()
    normalized_scope_ref = str(scope_ref or "").strip()
    resolved_department = await _resolve_scope_department(unit_id=unit_id, department=department)
    if normalized_scope_type == "nacional" or normalized_scope_ref == "Uruguay" or (not resolved_department and not normalized_scope_type):
        return {"scope_type": "nacional", "scope_ref": "Uruguay", "department": ""}
    return {
        "scope_type": "departamento",
        "scope_ref": resolved_department or normalized_scope_ref or "Uruguay",
        "department": resolved_department or normalized_scope_ref or "",
    }


async def _get_raster_product_by_key(product_key: str) -> RasterProduct | None:
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(RasterProduct).where(RasterProduct.product_key == product_key).limit(1))
            return result.scalar_one_or_none()
    except Exception:
        return None


async def _get_mosaic_by_key(mosaic_key: str) -> RasterMosaic | None:
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(RasterMosaic).where(RasterMosaic.mosaic_key == mosaic_key).limit(1))
            return result.scalar_one_or_none()
    except Exception:
        return None


async def get_department_cog_product(
    *,
    layer_id: str,
    display_date: date | str,
    department: str,
) -> RasterProduct | None:
    zoom = canonical_zoom_for_layer(layer_id)
    product_key = department_raster_product_key(layer_id=layer_id, display_date=display_date, department=department, zoom=zoom)
    row = await _get_raster_product_by_key(product_key)
    if row is None or row.status not in {"ready", "empty"}:
        return None
    return row


async def get_latest_ready_department_cog_before(
    *,
    layer_id: str,
    display_date: date | str,
    department: str,
    lookback_days: int = DEPARTMENT_CARRY_FORWARD_LOOKBACK_DAYS,
) -> RasterProduct | None:
    target_dt = _date_to_datetime(display_date)
    if target_dt is None:
        return None
    min_dt = target_dt - timedelta(days=max(int(lookback_days), 1))
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(RasterProduct).where(
                    RasterProduct.layer_id == layer_id,
                    RasterProduct.product_kind == "department_daily_cog",
                    RasterProduct.scope_type == "departamento",
                    RasterProduct.scope_ref == department,
                    RasterProduct.status == "ready",
                    RasterProduct.visual_empty == 0,
                    RasterProduct.display_date < target_dt,
                    RasterProduct.display_date >= min_dt,
                ).order_by(RasterProduct.display_date.desc()).limit(max(int(lookback_days), 1))
            )
            rows = result.scalars().all()
    except Exception:
        return None
    if not rows:
        return None
    for row in rows:
        metadata = _department_product_metadata(row)
        metadata.setdefault("layer_id", layer_id)
        if layer_id == "rgb" and "cloudlike_max_tile_pct" not in metadata:
            storage_path = await _ensure_storage_file(row.storage_key)
            if storage_path is not None and storage_path.exists():
                try:
                    with rasterio.open(storage_path) as dataset:
                        array_rgba = np.transpose(dataset.read(), (1, 2, 0))
                    metadata["cloudlike_pct"] = _cloudlike_pct_from_rgba(array_rgba)
                    metadata["cloudlike_max_tile_pct"] = _max_cloudlike_tile_pct_from_rgba(array_rgba)
                except Exception:
                    metadata["cloudlike_pct"] = 100.0
                    metadata["cloudlike_max_tile_pct"] = 100.0
        if layer_id == "rgb" and _metadata_rgb_cloud_degraded(metadata):
            continue
        return row
    return None


def _department_carry_forward_metadata(
    *,
    row: RasterProduct,
    layer_id: str,
    display_date: date,
) -> dict[str, Any]:
    metadata = _department_product_metadata(row)
    resolved_source_date = (
        row.source_date.date().isoformat()
        if row.source_date
        else (row.display_date.date().isoformat() if row.display_date else display_date.isoformat())
    )
    metadata.update(
        {
            "available": True,
            "availability": "historical_carry_forward",
            "is_interpolated": True,
            "label": "Interpolado",
            "visual_empty": False,
            "visual_state": "interpolated",
            "skip_in_playback": False,
            "empty_reason": None,
            "selection_reason": "department_product_carry_forward",
            "coverage_origin": "department_daily_cog_carry_forward",
            "resolved_source_date": resolved_source_date,
            "primary_source_date": resolved_source_date,
            "secondary_source_date": None,
            "blend_weight": 0.0,
            "source_locked": True,
            "fusion_mode": (
                "s1_s2_carry_forward"
                if str(layer_id or "").strip().lower() == "alerta_fusion"
                else metadata.get("fusion_mode")
            ),
            "s1_present": str(layer_id or "").strip().lower() == "alerta_fusion",
            "s2_present": str(layer_id or "").strip().lower() != "alerta_fusion",
            "s2_mask_valid": str(layer_id or "").strip().lower() != "alerta_fusion",
            "carry_forward_from_display_date": row.display_date.date().isoformat() if row.display_date else None,
        }
    )
    return metadata


async def _resolve_department_cog_product_for_date(
    *,
    layer_id: str,
    display_date: date,
    department: str,
) -> tuple[RasterProduct | None, dict[str, Any] | None]:
    row = await get_department_cog_product(layer_id=layer_id, display_date=display_date, department=department)
    if row is not None:
        return row, _department_product_metadata(row)
    if layer_id not in DEPARTMENT_CARRY_FORWARD_LAYERS:
        return None, None
    previous_row = await get_latest_ready_department_cog_before(
        layer_id=layer_id,
        display_date=display_date,
        department=department,
    )
    if previous_row is None:
        return None, None
    return previous_row, _department_carry_forward_metadata(
        row=previous_row,
        layer_id=layer_id,
        display_date=display_date,
    )


async def get_national_mosaic(
    *,
    layer_id: str,
    display_date: date | str,
) -> RasterMosaic | None:
    row = await _get_mosaic_by_key(national_mosaic_key(layer_id=layer_id, display_date=display_date))
    if row is None or row.status not in {"ready", "empty"}:
        return None
    return row


async def upsert_raster_mosaic(
    session: AsyncSession,
    *,
    mosaic_key: str,
    layer_id: str,
    display_date: date | str,
    storage_backend: str,
    storage_key: str | None,
    status: str,
    visual_empty: bool,
    source_product_keys: list[str],
    metadata_extra: dict[str, Any] | None = None,
) -> RasterMosaic:
    result = await session.execute(select(RasterMosaic).where(RasterMosaic.mosaic_key == mosaic_key).limit(1))
    row = result.scalar_one_or_none()
    if row is None:
        row = RasterMosaic(mosaic_key=mosaic_key, layer_id=layer_id)
        session.add(row)
    row.scope_type = "nacional"
    row.scope_ref = "Uruguay"
    row.display_date = _date_to_datetime(display_date)
    row.storage_backend = storage_backend
    row.storage_key = storage_key
    row.status = status
    row.visual_empty = 1 if visual_empty else 0
    row.source_product_keys = source_product_keys
    row.metadata_extra = metadata_extra or {}
    row.updated_at = _now_utc()
    await session.flush()
    return row


def _read_storage_path(storage_key: str | None) -> Path | None:
    if not storage_key:
        return None
    return _product_fs_path(storage_key)


async def _ensure_storage_file(storage_key: str | None) -> Path | None:
    fs_path = _read_storage_path(storage_key)
    if fs_path is None:
        return None
    if fs_path.exists():
        return fs_path
    bucket_cached = await storage_get_bytes(storage_key)
    if bucket_cached:
        try:
            fs_path.parent.mkdir(parents=True, exist_ok=True)
            fs_path.write_bytes(bucket_cached[0])
            return fs_path
        except Exception:
            return None
    return None


async def build_department_daily_cog(
    *,
    layer_id: str,
    display_date: date,
    department: str,
    force: bool = False,
) -> dict[str, Any]:
    zoom = canonical_zoom_for_layer(layer_id)
    bbox_bucket = _canonical_bbox_bucket("departamento", department, zoom)
    existing = None if force else await get_department_cog_product(layer_id=layer_id, display_date=display_date, department=department)
    if existing is not None:
        metadata = _department_product_metadata(existing)
        recent_date = display_date >= (date.today() - timedelta(days=7))
        can_retry_empty = (
            metadata.get("visual_empty", False)
            and layer_id in DEPARTMENT_CARRY_FORWARD_LAYERS
            and recent_date
        )
        if not can_retry_empty:
            return {
                "status": "reused",
                "product_key": existing.product_key,
                "storage_key": existing.storage_key,
                "visual_empty": metadata.get("visual_empty", False),
                "renderable_pixel_pct": metadata.get("renderable_pixel_pct", 0.0),
            }

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AOIUnit).where(
                AOIUnit.unit_type == "department",
                AOIUnit.department == department,
            ).limit(1)
        )
        department_unit = result.scalar_one_or_none()
    if department_unit is None or not department_unit.geometry_geojson:
        return {"status": "missing_department_geometry", "department": department, "layer_id": layer_id}

    geometry = _geometry_object(department_unit.geometry_geojson)
    if geometry is None:
        return {"status": "invalid_department_geometry", "department": department, "layer_id": layer_id}
    try:
        west, south, east, north = shape(geometry).bounds
    except Exception:
        return {"status": "invalid_department_geometry", "department": department, "layer_id": layer_id}
    bbox = f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"
    tile_coords = _tile_coords_for_bbox(bbox, zoom)
    if not tile_coords:
        return {"status": "empty_bbox", "department": department, "layer_id": layer_id}

    from app.services.public_api import (
        _carry_forward_runtime_metadata,
        _fetch_temporal_tile_attempt,
        _probe_runtime_bucket_source_metadata,
        _resolve_timeline_source_metadata,
        legacy_get_token,
    )

    async def _maybe_carry_forward_department_product(reason: str, base_metadata: dict[str, Any] | None = None) -> dict[str, Any] | None:
        if layer_id not in DEPARTMENT_CARRY_FORWARD_LAYERS:
            return None
        previous_product = await get_latest_ready_department_cog_before(
            layer_id=layer_id,
            display_date=display_date,
            department=department,
        )
        if previous_product is None:
            return None
        previous_path = await _ensure_storage_file(previous_product.storage_key)
        if previous_path is None or not previous_path.exists():
            return None
        try:
            content = previous_path.read_bytes()
        except Exception:
            return None
        if not content:
            return None
        previous_metadata = _department_product_metadata(previous_product)
        carried_metadata = _carry_forward_runtime_metadata(
            layer=layer_id,
            display_date=display_date,
            metadata=previous_metadata,
            carry_from_date=previous_product.display_date.date() if previous_product.display_date else display_date,
        )
        carried_metadata.update(
            {
                **_base_raster_build_metadata(layer_id),
                "department": department,
                "bbox": bbox,
                "bbox_bucket": bbox_bucket,
                "canonical_zoom": zoom,
                "coverage_origin": "department_daily_cog_carry_forward",
                "selection_reason": "runtime_bucket_carry_forward",
                "carry_forward_reason": reason,
                "carry_forward_from_product_key": previous_product.product_key,
                "source_locked": True,
            }
        )
        if base_metadata:
            carried_metadata["carry_forward_probe_state"] = {
                "selection_reason": base_metadata.get("selection_reason"),
                "visual_state": base_metadata.get("visual_state"),
                "visual_empty": base_metadata.get("visual_empty"),
                "resolved_source_date": base_metadata.get("resolved_source_date"),
                "empty_reason": base_metadata.get("empty_reason"),
            }
        storage_key = department_raster_storage_key(layer_id=layer_id, display_date=display_date, department=department)
        fs_path = _product_fs_path(storage_key)
        fs_path.parent.mkdir(parents=True, exist_ok=True)
        fs_path.write_bytes(content)
        await storage_put_bytes(storage_key, content, content_type="image/tiff")
        product_key = department_raster_product_key(layer_id=layer_id, display_date=display_date, department=department, zoom=zoom)
        async with AsyncSessionLocal() as session:
            await upsert_raster_product(
                session,
                product_key=product_key,
                layer_id=layer_id,
                product_kind="department_daily_cog",
                scope_type="departamento",
                scope_ref=department,
                display_date=display_date,
                source_date=previous_product.source_date.date() if previous_product.source_date else None,
                zoom=zoom,
                bbox_bucket=bbox_bucket,
                storage_backend="filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem",
                storage_key=storage_key,
                content_type=previous_product.content_type or "image/tiff",
                width=int(previous_product.width or 0),
                height=int(previous_product.height or 0),
                tile_min_x=int(previous_product.tile_min_x or 0),
                tile_min_y=int(previous_product.tile_min_y or 0),
                tile_max_x=int(previous_product.tile_max_x or 0),
                tile_max_y=int(previous_product.tile_max_y or 0),
                visual_empty=False,
                status="ready",
                bytes_size=len(content),
                metadata_extra=carried_metadata,
            )
            await session.commit()
        return {
            "status": "carried_forward",
            "product_key": product_key,
            "storage_key": storage_key,
            "visual_empty": False,
            "renderable_pixel_pct": carried_metadata.get("renderable_pixel_pct", 0.0),
            "resolved_source_date": carried_metadata.get("resolved_source_date"),
        }

    if layer_id in NEAR_REAL_TIME_CARRY_FORWARD_LAYERS and display_date >= (date.today() - timedelta(days=2)):
        carried_product = await _maybe_carry_forward_department_product(reason="near_real_time_department_carry_forward")
        if carried_product is not None:
            return carried_product

    source_metadata = await _resolve_timeline_source_metadata(
        layer_id,
        display_date,
        bbox_bucket=bbox_bucket,
        bbox=bbox,
        zoom=zoom,
        scope="departamento",
        department=department,
        scope_type="departamento",
        scope_ref=department,
        allow_runtime_probe=True,
    )
    if layer_id in {"rgb", "ndvi", "ndmi", "ndwi", "savi", "alerta_fusion"} and str(source_metadata.get("selection_reason") or "") == "snapshot_exact":
        probe_seed_metadata = {
            **source_metadata,
            "selection_reason": "heuristic_fallback",
            "source_locked": False,
            "resolved_source_date": None,
        }
        probed_metadata = await _probe_runtime_bucket_source_metadata(
            layer=layer_id,
            display_date=display_date,
            bbox=bbox,
            zoom=zoom,
            fallback_metadata=probe_seed_metadata,
            scope="departamento",
            unit_id=department_unit.id,
            department=department,
            scope_type="departamento",
            scope_ref=department,
            bbox_bucket=bbox_bucket,
        )
        if probed_metadata is not None:
            source_metadata = {
                **source_metadata,
                **probed_metadata,
                "coverage_origin": "department_daily_cog_runtime_probe",
            }
    if layer_id in DEPARTMENT_CARRY_FORWARD_LAYERS and str(source_metadata.get("selection_reason") or "") == "snapshot_exact":
        carried_product = await _maybe_carry_forward_department_product(
            reason="snapshot_exact_not_promoted_by_runtime_probe",
            base_metadata=source_metadata,
        )
        if carried_product is not None:
            return carried_product
    if source_metadata.get("visual_empty") or source_metadata.get("visual_state") in {"empty", "missing"}:
        carried_product = await _maybe_carry_forward_department_product(
            reason=str(source_metadata.get("empty_reason") or source_metadata.get("selection_reason") or "source_metadata_empty"),
            base_metadata=source_metadata,
        )
        if carried_product is not None:
            return carried_product
        product_key = department_raster_product_key(layer_id=layer_id, display_date=display_date, department=department, zoom=zoom)
        storage_key = department_raster_storage_key(layer_id=layer_id, display_date=display_date, department=department)
        metadata_extra = {
            **_base_raster_build_metadata(layer_id),
            **source_metadata,
            "department": department,
            "bbox": bbox,
            "bbox_bucket": bbox_bucket,
            "canonical_zoom": zoom,
        }
        async with AsyncSessionLocal() as session:
            await upsert_raster_product(
                session,
                product_key=product_key,
                layer_id=layer_id,
                product_kind="department_daily_cog",
                scope_type="departamento",
                scope_ref=department,
                display_date=display_date,
                source_date=source_metadata.get("resolved_source_date") or source_metadata.get("primary_source_date"),
                zoom=zoom,
                bbox_bucket=bbox_bucket,
                storage_backend="filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem",
                storage_key=storage_key,
                content_type="image/tiff",
                width=0,
                height=0,
                tile_min_x=0,
                tile_min_y=0,
                tile_max_x=0,
                tile_max_y=0,
                visual_empty=True,
                status="empty",
                bytes_size=0,
                metadata_extra=metadata_extra,
            )
            await session.commit()
        return {"status": "empty", "product_key": product_key, "visual_empty": True}

    if legacy_get_token is None:
        return {"status": "missing_token_provider", "department": department, "layer_id": layer_id}

    resolved_source = str(source_metadata.get("resolved_source_date") or source_metadata.get("primary_source_date") or display_date.isoformat())
    try:
        source_date = date.fromisoformat(resolved_source)
    except Exception:
        source_date = display_date

    token = await asyncio.to_thread(legacy_get_token)
    xs = sorted({x for x, _ in tile_coords})
    ys = sorted({y for _, y in tile_coords})
    min_x, max_x = xs[0], xs[-1]
    min_y, max_y = ys[0], ys[-1]
    width = (max_x - min_x + 1) * TILE_SIZE
    height = (max_y - min_y + 1) * TILE_SIZE
    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    had_any_tile = False

    for tile_x, tile_y in tile_coords:
        content = await _fetch_temporal_tile_attempt(
            layer=layer_id,
            z=zoom,
            x=tile_x,
            y=tile_y,
            display_date=display_date,
            source_date=source_date,
            widen_window=False,
            token=token,
            frame_role="primary",
            source_metadata=source_metadata,
            scope="departamento",
            unit_id=department_unit.id,
            department=department,
            scope_type="departamento",
            scope_ref=department,
            bbox_bucket=bbox_bucket,
            use_internal_products=False,
        )
        if not content:
            continue
        try:
            with Image.open(io.BytesIO(content)) as image:
                rgba = image.convert("RGBA")
                canvas.paste(rgba, ((tile_x - min_x) * TILE_SIZE, (tile_y - min_y) * TILE_SIZE))
                had_any_tile = True
        except Exception:
            continue

    array_rgba = np.array(canvas, dtype=np.uint8)
    visible_pct = _renderable_pct_from_rgba(array_rgba) if had_any_tile else 0.0
    cloudlike_pct = _cloudlike_pct_from_rgba(array_rgba) if layer_id == "rgb" and had_any_tile else 0.0
    cloudlike_max_tile_pct = _max_cloudlike_tile_pct_from_rgba(array_rgba) if layer_id == "rgb" and had_any_tile else 0.0
    visual_empty = visible_pct < _visual_empty_product_threshold_pct(layer_id)
    rgb_cloud_degraded = layer_id == "rgb" and cloudlike_max_tile_pct >= _max_cloudlike_tile_threshold_pct(layer_id)
    if visual_empty:
        carried_product = await _maybe_carry_forward_department_product(
            reason="rendered_product_visually_empty",
            base_metadata={
                **source_metadata,
                "layer_id": layer_id,
                "renderable_pixel_pct": visible_pct,
                "visual_empty": True,
                "visual_state": "empty",
                "empty_reason": "rendered_product_visually_empty",
            },
        )
        if carried_product is not None:
            return carried_product
    if rgb_cloud_degraded:
        carried_product = await _maybe_carry_forward_department_product(
            reason="rendered_product_cloud_degraded",
            base_metadata={
                **source_metadata,
                "layer_id": layer_id,
                "renderable_pixel_pct": visible_pct,
                "visual_empty": True,
                "visual_state": "empty",
                "empty_reason": "rendered_product_cloud_degraded",
                "cloudlike_pct": cloudlike_pct,
                "cloudlike_max_tile_pct": cloudlike_max_tile_pct,
            },
        )
        if carried_product is not None:
            return carried_product
        visual_empty = True
    product_key = department_raster_product_key(layer_id=layer_id, display_date=display_date, department=department, zoom=zoom)
    storage_key = department_raster_storage_key(layer_id=layer_id, display_date=display_date, department=department)
    fs_path = _product_fs_path(storage_key)
    fs_path.parent.mkdir(parents=True, exist_ok=True)

    min_bounds = _tile_xy_bounds(min_x, min_y, zoom)
    max_bounds = _tile_xy_bounds(max_x, max_y, zoom)
    left, _, _, top = min_bounds
    _, bottom, right, _ = max_bounds
    transform = from_bounds(left, bottom, right, top, width, height)

    bands = np.transpose(array_rgba, (2, 0, 1))
    profile = {
        "driver": "COG",
        "width": width,
        "height": height,
        "count": 4,
        "dtype": rasterio.uint8,
        "crs": "EPSG:3857",
        "transform": transform,
        "compress": "DEFLATE",
        "blocksize": 512,
        "overview_resampling": "average",
    }
    try:
        with rasterio.open(fs_path, "w", **profile) as dataset:
            dataset.write(bands)
    except Exception:
        fallback_profile = {
            "driver": "GTiff",
            "width": width,
            "height": height,
            "count": 4,
            "dtype": rasterio.uint8,
            "crs": "EPSG:3857",
            "transform": transform,
            "tiled": True,
            "compress": "DEFLATE",
            "blockxsize": 512,
            "blockysize": 512,
        }
        with rasterio.open(fs_path, "w", **fallback_profile) as dataset:
            dataset.write(bands)
            dataset.build_overviews([2, 4, 8, 16], Resampling.average)
            dataset.update_tags(ns="rio_overview", resampling="average")
    content = fs_path.read_bytes()
    await storage_put_bytes(storage_key, content, content_type="image/tiff")

    metadata_extra = {
        **_base_raster_build_metadata(layer_id),
        **source_metadata,
        "department": department,
        "bbox": bbox,
        "bbox_bucket": bbox_bucket,
        "canonical_zoom": zoom,
        "layer_id": layer_id,
        "renderable_pixel_pct": visible_pct,
        "visible_pixel_pct": visible_pct,
        "cloudlike_pct": cloudlike_pct,
        "cloudlike_max_tile_pct": cloudlike_max_tile_pct,
        "coverage_origin": "department_daily_cog",
        "resolved_source_date": source_date.isoformat(),
        "tile_min_x": min_x,
        "tile_min_y": min_y,
        "tile_max_x": max_x,
        "tile_max_y": max_y,
    }
    if visual_empty and rgb_cloud_degraded:
        metadata_extra["visual_state"] = "empty"
        metadata_extra["visual_empty"] = True
        metadata_extra["empty_reason"] = "rendered_product_cloud_degraded"
    async with AsyncSessionLocal() as session:
        await upsert_raster_product(
            session,
            product_key=product_key,
            layer_id=layer_id,
            product_kind="department_daily_cog",
            scope_type="departamento",
            scope_ref=department,
            display_date=display_date,
            source_date=source_date,
            zoom=zoom,
            bbox_bucket=bbox_bucket,
            storage_backend="filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem",
            storage_key=storage_key,
            content_type="image/tiff",
            width=width,
            height=height,
            tile_min_x=min_x,
            tile_min_y=min_y,
            tile_max_x=max_x,
            tile_max_y=max_y,
            visual_empty=visual_empty,
            status="empty" if visual_empty else "ready",
            bytes_size=len(content),
            metadata_extra=metadata_extra,
        )
        await session.commit()
    return {
        "status": "ready" if not visual_empty else "empty",
        "product_key": product_key,
        "storage_key": storage_key,
        "renderable_pixel_pct": visible_pct,
        "visual_empty": visual_empty,
        "canonical_zoom": zoom,
    }


async def build_national_mosaic(
    *,
    layer_id: str,
    display_date: date,
    force: bool = False,
) -> dict[str, Any]:
    existing = None if force else await get_national_mosaic(layer_id=layer_id, display_date=display_date)
    if existing is not None:
        metadata = _mosaic_metadata(existing)
        return {"status": "reused", "mosaic_key": existing.mosaic_key, "visual_empty": metadata.get("visual_empty", False)}

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(RasterProduct).where(
                RasterProduct.layer_id == layer_id,
                RasterProduct.product_kind == "department_daily_cog",
                RasterProduct.scope_type == "departamento",
                RasterProduct.display_date == _date_to_datetime(display_date),
                RasterProduct.status.in_(["ready", "empty"]),
            )
        )
        department_products = result.scalars().all()
        if not department_products:
            await upsert_raster_mosaic(
                session,
                mosaic_key=national_mosaic_key(layer_id=layer_id, display_date=display_date),
                layer_id=layer_id,
                display_date=display_date,
                storage_backend="filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem",
                storage_key=national_mosaic_storage_key(layer_id=layer_id, display_date=display_date),
                status="empty",
                visual_empty=True,
                source_product_keys=[],
                metadata_extra={
                    **_base_raster_build_metadata(layer_id),
                    "layer_id": layer_id,
                    "display_date": display_date.isoformat(),
                    "asset_count": 0,
                    "usable_asset_count": 0,
                    "department_asset_count": 0,
                    "coverage_origin": "national_mosaic",
                    "resolved_source_date": None,
                },
            )
            await session.commit()
            return {"status": "empty", "mosaic_key": national_mosaic_key(layer_id=layer_id, display_date=display_date), "asset_count": 0}

        storage_key = national_mosaic_storage_key(layer_id=layer_id, display_date=display_date)
        usable_products = [row for row in department_products if _department_product_is_usable_for_mosaic(row)]
        asset_entries = [_department_asset_entry(row) for row in usable_products]
        visual_empty = len(asset_entries) <= 0
        fs_path = _product_fs_path(storage_key)
        fs_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "mosaic": "agroclimax-national-v1",
            "layer_id": layer_id,
            "display_date": display_date.isoformat(),
            "scope_type": "nacional",
            "scope_ref": "Uruguay",
            "assets": asset_entries,
        }
        fs_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
        await storage_put_bytes(storage_key, fs_path.read_bytes(), content_type="application/json")
        await upsert_raster_mosaic(
            session,
            mosaic_key=national_mosaic_key(layer_id=layer_id, display_date=display_date),
            layer_id=layer_id,
            display_date=display_date,
            storage_backend="filesystem+object_storage" if settings.storage_bucket_enabled else "filesystem",
            storage_key=storage_key,
            status="empty" if visual_empty else "ready",
            visual_empty=visual_empty,
            source_product_keys=[row.product_key for row in department_products],
            metadata_extra={
                **_base_raster_build_metadata(layer_id),
                "asset_count": len(asset_entries),
                "usable_asset_count": len(asset_entries),
                "department_asset_count": len(department_products),
                "assets": asset_entries,
                "coverage_origin": "national_mosaic",
                "resolved_source_date": display_date.isoformat(),
            },
        )
        await session.commit()
    return {
        "status": "empty" if visual_empty else "ready",
        "mosaic_key": national_mosaic_key(layer_id=layer_id, display_date=display_date),
        "asset_count": len(asset_entries),
        "visual_empty": visual_empty,
    }


async def get_canonical_product_frame_metadata(
    *,
    layer_id: str,
    display_date: date,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, Any] | None:
    scope = await resolve_canonical_product_scope(
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    if scope["scope_type"] == "nacional":
        row = await get_national_mosaic(layer_id=layer_id, display_date=display_date)
        if row is not None:
            metadata = _mosaic_metadata(row)
            metadata.pop("assets", None)
            return metadata
        _, metadata = await _get_runtime_national_mosaic_assets(layer_id=layer_id, display_date=display_date)
        if int(metadata.get("department_asset_count") or 0) <= 0:
            return None
        return metadata
    resolved_department = scope["department"] or scope["scope_ref"]
    _, metadata = await _resolve_department_cog_product_for_date(
        layer_id=layer_id,
        display_date=display_date,
        department=resolved_department,
    )
    return metadata


async def get_canonical_product_status_index(
    *,
    layer_ids: list[str],
    date_from: date,
    date_to: date,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, dict[str, dict[str, Any]]]:
    if not layer_ids:
        return {}
    scope = await resolve_canonical_product_scope(
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    start_dt = _date_to_datetime(date_from)
    end_dt = datetime.combine(date_to, time.min, tzinfo=timezone.utc) + timedelta(days=1)
    index: dict[str, dict[str, dict[str, Any]]] = {}
    try:
        async with AsyncSessionLocal() as session:
            if scope["scope_type"] == "nacional":
                mosaic_result = await session.execute(
                    select(RasterMosaic).where(
                        RasterMosaic.layer_id.in_(layer_ids),
                        RasterMosaic.display_date >= start_dt,
                        RasterMosaic.display_date < end_dt,
                        RasterMosaic.scope_type == "nacional",
                        RasterMosaic.scope_ref == "Uruguay",
                    )
                )
                mosaics: dict[tuple[str, str], dict[str, Any]] = {}
                for row in mosaic_result.scalars().all():
                    meta = _mosaic_metadata(row)
                    meta.pop("assets", None)
                    mosaics[(str(row.layer_id), row.display_date.date().isoformat())] = meta

                dept_result = await session.execute(
                    select(RasterProduct).where(
                        RasterProduct.layer_id.in_(layer_ids),
                        RasterProduct.product_kind == "department_daily_cog",
                        RasterProduct.scope_type == "departamento",
                        RasterProduct.display_date >= start_dt,
                        RasterProduct.display_date < end_dt,
                        RasterProduct.status.in_(["ready", "empty"]),
                    )
                )
                dept_rows = list(dept_result.scalars().all())
                grouped: dict[tuple[str, str], list[RasterProduct]] = {}
                for row in dept_rows:
                    if row.display_date is None:
                        continue
                    key = (str(row.layer_id), row.display_date.date().isoformat())
                    grouped.setdefault(key, []).append(row)

                for (layer_id, day), rows in grouped.items():
                    usable_rows = [row for row in rows if _department_product_is_usable_for_mosaic(row)]
                    visual_empty = len(usable_rows) <= 0
                    if usable_rows:
                        renderable_pct = max(
                            _stored_product_visible_pct(dict(row.metadata_extra or {}))
                            for row in usable_rows
                        )
                    else:
                        renderable_pct = 0.0
                    aggregated = {
                        "layer_id": layer_id,
                        "display_date": day,
                        "scope_type": "nacional",
                        "scope_ref": "Uruguay",
                        "asset_count": len(usable_rows),
                        "usable_asset_count": len(usable_rows),
                        "department_asset_count": len(rows),
                        "renderable_pixel_pct": renderable_pct,
                        "visual_empty": visual_empty,
                        "visual_state": "empty" if visual_empty else "ready",
                        "coverage_origin": "national_mosaic_runtime",
                        "cache_status": "empty" if visual_empty else "ready",
                    }
                    layer_index = index.setdefault(layer_id, {})
                    layer_index[day] = mosaics.get((layer_id, day), aggregated)

                # Ensure any materialized mosaics still appear even if department rows are missing.
                for (layer_id, day), meta in mosaics.items():
                    layer_index = index.setdefault(layer_id, {})
                    layer_index.setdefault(day, meta)
            else:
                department_ref = scope["department"] or scope["scope_ref"]
                result = await session.execute(
                    select(RasterProduct).where(
                        RasterProduct.layer_id.in_(layer_ids),
                        RasterProduct.product_kind == "department_daily_cog",
                        RasterProduct.display_date >= start_dt,
                        RasterProduct.display_date < end_dt,
                        RasterProduct.scope_type == "departamento",
                        RasterProduct.scope_ref == department_ref,
                    )
                )
                for row in result.scalars().all():
                    layer_index = index.setdefault(str(row.layer_id), {})
                    layer_index[row.display_date.date().isoformat()] = _department_product_metadata(row)
    except Exception:
        return {}
    return index


def _image_data_to_png(image_data) -> bytes:
    data = np.transpose(image_data.data, (1, 2, 0))
    mask = image_data.mask
    if mask is None:
        alpha = np.full((data.shape[0], data.shape[1]), 255, dtype=np.uint8)
    else:
        alpha = np.where(mask > 0, 255, 0).astype(np.uint8)
    rgba = np.dstack([data[:, :, :3], alpha])
    buffer = io.BytesIO()
    Image.fromarray(rgba, mode="RGBA").save(buffer, format="PNG")
    return buffer.getvalue()


async def _read_cog_tile(storage_key: str | None, *, x: int, y: int, z: int) -> tuple[bytes | None, dict[str, Any] | None]:
    fs_path = await _ensure_storage_file(storage_key)
    if fs_path is None or not fs_path.exists():
        return None, None
    try:
        with Reader(fs_path) as cog:
            tile = cog.tile(x, y, z, tilesize=TILE_SIZE)
            return _image_data_to_png(tile), {"reader": "rio-tiler", "storage_key": storage_key}
    except Exception as exc:
        reader_error = f"{type(exc).__name__}: {exc}"
    try:
        left, bottom, right, top = _tile_xy_bounds(x, y, z)
        with rasterio.open(fs_path) as dataset:
            window = rasterio.windows.from_bounds(left, bottom, right, top, transform=dataset.transform)
            resampling = Resampling.bilinear if int(dataset.count or 0) >= 3 else Resampling.nearest
            band_count = max(1, min(int(dataset.count or 0), 4))
            data = dataset.read(
                indexes=list(range(1, band_count + 1)),
                window=window,
                out_shape=(band_count, TILE_SIZE, TILE_SIZE),
                boundless=True,
                fill_value=0,
                resampling=resampling,
            )
        if data.size <= 0:
            return None, {"reader": "rasterio-window", "storage_key": storage_key, "reader_error": reader_error}
        if data.shape[0] == 4:
            rgba = np.transpose(data, (1, 2, 0)).astype(np.uint8, copy=False)
        elif data.shape[0] == 3:
            rgb = np.transpose(data, (1, 2, 0)).astype(np.uint8, copy=False)
            alpha = np.where(np.any(rgb > 0, axis=2), 255, 0).astype(np.uint8)
            rgba = np.dstack([rgb, alpha])
        else:
            mono = data[0].astype(np.uint8, copy=False)
            alpha = np.where(mono > 0, 255, 0).astype(np.uint8)
            rgba = np.dstack([mono, mono, mono, alpha])
        if int(np.count_nonzero(rgba[:, :, 3] > 0)) <= 0:
            return None, {"reader": "rasterio-window", "storage_key": storage_key, "reader_error": reader_error}
        buffer = io.BytesIO()
        Image.fromarray(rgba, mode="RGBA").save(buffer, format="PNG")
        return buffer.getvalue(), {"reader": "rasterio-window", "storage_key": storage_key, "reader_error": reader_error}
    except Exception:
        return None, None


async def read_department_cog_tile(
    *,
    layer_id: str,
    display_date: date,
    department: str,
    x: int,
    y: int,
    z: int,
) -> tuple[bytes | None, dict[str, Any] | None]:
    row, metadata = await _resolve_department_cog_product_for_date(
        layer_id=layer_id,
        display_date=display_date,
        department=department,
    )
    if row is None or metadata is None:
        return await read_scope_viewport_raster_fallback_tile(
            layer_id=layer_id,
            display_date=display_date,
            scope_type="departamento",
            scope_ref=department,
            x=x,
            y=y,
            z=z,
        )
    visible_pct = _stored_product_visible_pct(metadata)
    effective_visual_empty = _effective_row_visual_empty(layer_id, bool(row.visual_empty), metadata)
    if (row.status == "empty" or effective_visual_empty) and visible_pct <= 0.0:
        empty_metadata = {
            **metadata,
            "visual_empty": True,
            "visual_state": "empty",
            "empty_reason": str(metadata.get("empty_reason") or "department_product_empty"),
        }
        return TRANSPARENT_TILE_PNG, empty_metadata
    content, transport = await _read_cog_tile(row.storage_key, x=x, y=y, z=z)
    if transport:
        metadata.update(transport)
    if not content:
        fallback_content, fallback_metadata = await read_scope_viewport_raster_fallback_tile(
            layer_id=layer_id,
            display_date=display_date,
            scope_type="departamento",
            scope_ref=department,
            x=x,
            y=y,
            z=z,
        )
        if fallback_content and fallback_metadata is not None:
            return fallback_content, fallback_metadata
        empty_metadata = {
            **metadata,
            "visual_empty": True,
            "visual_state": "empty",
            "empty_reason": str(metadata.get("empty_reason") or "tile_outside_department_product"),
        }
        return TRANSPARENT_TILE_PNG, empty_metadata
    return content, metadata


async def read_national_mosaic_tile(
    *,
    layer_id: str,
    display_date: date,
    x: int,
    y: int,
    z: int,
) -> tuple[bytes | None, dict[str, Any] | None]:
    row = await get_national_mosaic(layer_id=layer_id, display_date=display_date)
    assets: list[dict[str, Any]] = []
    if row is not None:
        metadata = _mosaic_metadata(row)
        if row.visual_empty or row.status == "empty":
            empty_metadata = {
                **metadata,
                "visual_empty": True,
                "visual_state": "empty",
                "empty_reason": str(metadata.get("empty_reason") or "national_mosaic_empty"),
            }
            return TRANSPARENT_TILE_PNG, empty_metadata
        assets = list((row.metadata_extra or {}).get("assets") or [])
    else:
        assets, metadata = await _get_runtime_national_mosaic_assets(layer_id=layer_id, display_date=display_date)
        # No department products at all: let upstream fallback to remote serving.
        if int(metadata.get("department_asset_count") or 0) <= 0:
            return None, None
        if bool(metadata.get("visual_empty")):
            empty_metadata = {
                **metadata,
                "visual_empty": True,
                "visual_state": "empty",
                "empty_reason": str(metadata.get("empty_reason") or "national_mosaic_runtime_empty"),
            }
            return TRANSPARENT_TILE_PNG, empty_metadata
    intersecting_assets = [asset for asset in assets if _asset_intersects_tile(asset, x=x, y=y, z=z)]
    if not intersecting_assets:
        empty_metadata = {
            **metadata,
            "visual_empty": True,
            "visual_state": "empty",
            "empty_reason": str(metadata.get("empty_reason") or "national_mosaic_tile_outside_assets"),
            "rendered_assets": 0,
            "candidate_assets": 0,
        }
        return TRANSPARENT_TILE_PNG, empty_metadata
    canvas = Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0))
    rendered_assets = 0
    for asset in intersecting_assets:
        storage_key = str(asset.get("storage_key") or "")
        content, _ = await _read_cog_tile(storage_key, x=x, y=y, z=z)
        if not content:
            continue
        try:
            with Image.open(io.BytesIO(content)) as image:
                rgba = image.convert("RGBA")
                canvas.alpha_composite(rgba)
                rendered_assets += 1
        except Exception:
            continue
    if rendered_assets <= 0:
        empty_metadata = {
            **metadata,
            "visual_empty": True,
            "visual_state": "empty",
            "empty_reason": str(metadata.get("empty_reason") or "national_mosaic_tile_outside_assets"),
            "rendered_assets": 0,
            "candidate_assets": len(intersecting_assets),
        }
        return TRANSPARENT_TILE_PNG, empty_metadata
    buffer = io.BytesIO()
    canvas.save(buffer, format="PNG")
    metadata["rendered_assets"] = rendered_assets
    metadata["candidate_assets"] = len(intersecting_assets)
    return buffer.getvalue(), metadata


async def render_canonical_raster_tile(
    *,
    layer_id: str,
    display_date: date,
    x: int,
    y: int,
    z: int,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> tuple[bytes | None, dict[str, Any] | None]:
    scope = await resolve_canonical_product_scope(
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    if scope["scope_type"] == "nacional":
        return await read_national_mosaic_tile(layer_id=layer_id, display_date=display_date, x=x, y=y, z=z)
    resolved_department = scope["department"] or scope["scope_ref"]
    return await read_department_cog_tile(
        layer_id=layer_id,
        display_date=display_date,
        department=resolved_department,
        x=x,
        y=y,
        z=z,
    )
