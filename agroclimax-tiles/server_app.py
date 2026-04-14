from __future__ import annotations

import hashlib
import io
import json
import sys
from datetime import date
from pathlib import Path
import time
import unicodedata
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, Response
from PIL import Image
import numpy as np
from rio_tiler.io import Reader


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = REPO_ROOT / "apps" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import settings  # noqa: E402
from app.services.object_storage import (  # noqa: E402
    storage_get_bytes,
    storage_get_presigned_url,
)


app = FastAPI(title="AgroClimaX Tiles", version="0.1.0")

TILE_SIZE = 256


def _build_transparent_tile_png() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0)).save(buffer, format="PNG")
    return buffer.getvalue()


TRANSPARENT_TILE_PNG = _build_transparent_tile_png()


def _normalize_department_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_only = ascii_only.lower().replace("-", " ").replace("_", " ").strip()
    return " ".join(part for part in ascii_only.split(" ") if part)


_DEPARTMENT_CANONICAL_MAP: dict[str, str] = {
    _normalize_department_name("Artigas"): "Artigas",
    _normalize_department_name("Canelones"): "Canelones",
    _normalize_department_name("Cerro Largo"): "Cerro Largo",
    _normalize_department_name("Colonia"): "Colonia",
    _normalize_department_name("Durazno"): "Durazno",
    _normalize_department_name("Flores"): "Flores",
    _normalize_department_name("Florida"): "Florida",
    _normalize_department_name("Lavalleja"): "Lavalleja",
    _normalize_department_name("Maldonado"): "Maldonado",
    _normalize_department_name("Montevideo"): "Montevideo",
    _normalize_department_name("Paysandu"): "Paysandu",
    _normalize_department_name("Rio Negro"): "Rio Negro",
    _normalize_department_name("Rivera"): "Rivera",
    _normalize_department_name("Rocha"): "Rocha",
    _normalize_department_name("Salto"): "Salto",
    _normalize_department_name("San Jose"): "San Jose",
    _normalize_department_name("Soriano"): "Soriano",
    _normalize_department_name("Tacuarembo"): "Tacuarembo",
    _normalize_department_name("Treinta y Tres"): "Treinta y Tres",
}


def _canonical_department_name(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    normalized = _normalize_department_name(raw_value)
    if normalized in _DEPARTMENT_CANONICAL_MAP:
        return _DEPARTMENT_CANONICAL_MAP[normalized]
    # Accept already canonical-looking strings.
    return str(raw_value).strip()


def _department_raster_storage_key(*, layer_id: str, display_date: date, department: str) -> str:
    safe_department = hashlib.sha256(str(department).encode("utf-8")).hexdigest()[:20]
    build_version = str(settings.raster_product_build_version or "raster-v1")
    return f"raster-products/cogs/{build_version}/{layer_id}/{display_date.isoformat()}/{safe_department}.tif"


def _national_mosaic_storage_key(*, layer_id: str, display_date: date) -> str:
    build_version = str(settings.raster_product_build_version or "raster-v1")
    return f"raster-products/mosaics/{build_version}/{layer_id}/{display_date.isoformat()}/national_mosaic.json"


def _local_product_path(storage_key: str) -> Path:
    # Mirrors apps/backend/app/services/raster_products.py storage layout.
    backend_app_root = BACKEND_ROOT / "app"
    return backend_app_root / ".raster_products" / Path(storage_key)


_MOSAIC_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_PRESIGN_CACHE: dict[str, tuple[float, str]] = {}


async def _get_presigned_url_cached(storage_key: str, *, expires_seconds: int = 900) -> str | None:
    now = time.time()
    cached = _PRESIGN_CACHE.get(storage_key)
    if cached and cached[0] > now:
        return cached[1]
    url = await storage_get_presigned_url(storage_key, expires_seconds=expires_seconds)
    if not url:
        return None
    # Keep a small safety margin.
    _PRESIGN_CACHE[storage_key] = (now + max(30, int(expires_seconds) - 60), url)
    return url


async def _open_cog_source(storage_key: str) -> str | Path | None:
    if settings.storage_bucket_enabled:
        return await _get_presigned_url_cached(storage_key)
    fs_path = _local_product_path(storage_key)
    return fs_path if fs_path.exists() else None


async def _load_mosaic_payload(layer_id: str, display_date: date) -> dict[str, Any] | None:
    cache_key = f"{layer_id}:{display_date.isoformat()}"
    now = time.time()
    cached = _MOSAIC_CACHE.get(cache_key)
    if cached and cached[0] > now:
        return cached[1]

    storage_key = _national_mosaic_storage_key(layer_id=layer_id, display_date=display_date)
    payload: dict[str, Any] | None = None
    if settings.storage_bucket_enabled:
        bucket = await storage_get_bytes(storage_key)
        if bucket:
            try:
                payload = json.loads(bucket[0].decode("utf-8"))
            except Exception:
                payload = None
    else:
        fs_path = _local_product_path(storage_key)
        if fs_path.exists():
            try:
                payload = json.loads(fs_path.read_text(encoding="utf-8"))
            except Exception:
                payload = None

    if payload is not None:
        _MOSAIC_CACHE[cache_key] = (now + 60.0, payload)
    return payload


def _image_data_to_rgba(image_data) -> np.ndarray:
    data = np.transpose(image_data.data, (1, 2, 0)).astype(np.uint8, copy=False)
    mask = image_data.mask
    band_count = int(data.shape[2]) if data.ndim == 3 else 0
    if band_count <= 0:
        return np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)

    if band_count >= 3:
        rgb = data[:, :, :3]
    else:
        mono = data[:, :, 0]
        rgb = np.dstack([mono, mono, mono]).astype(np.uint8, copy=False)

    if mask is not None:
        alpha = np.where(mask > 0, 255, 0).astype(np.uint8)
    elif band_count >= 4:
        alpha = data[:, :, 3].astype(np.uint8, copy=False)
    else:
        alpha = np.where(np.any(rgb > 0, axis=2), 255, 0).astype(np.uint8)

    return np.dstack([rgb, alpha]).astype(np.uint8, copy=False)


def _rgba_to_png(rgba: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    Image.fromarray(rgba, mode="RGBA").save(buffer, format="PNG")
    return buffer.getvalue()


def _renderable_pct_from_rgba(rgba: np.ndarray) -> float:
    if rgba.size <= 0:
        return 0.0
    alpha = rgba[:, :, 3]
    total_pixels = max(int(alpha.shape[0] * alpha.shape[1]), 1)
    opaque_pixels = int(np.count_nonzero(alpha > 0))
    return round((opaque_pixels / total_pixels) * 100.0, 2)


async def _read_cog_tile(storage_key: str, *, x: int, y: int, z: int) -> tuple[bytes | None, dict[str, Any]]:
    source = await _open_cog_source(storage_key)
    if source is None:
        return None, {"storage_key": storage_key, "reader": "missing_source"}
    try:
        with Reader(source) as cog:
            tile = cog.tile(x, y, z, tilesize=TILE_SIZE)
        rgba = _image_data_to_rgba(tile)
        return _rgba_to_png(rgba), {
            "storage_key": storage_key,
            "reader": "rio-tiler",
            "renderable_pixel_pct": _renderable_pct_from_rgba(rgba),
        }
    except Exception as exc:
        return None, {"storage_key": storage_key, "reader": "rio-tiler", "reader_error": f"{type(exc).__name__}: {exc}"}


def _asset_intersects_tile(asset: dict[str, Any], *, x: int, y: int, z: int) -> bool:
    # Keep identical semantics to backend raster_products._asset_intersects_tile.
    try:
        bbox_value = asset.get("bbox")
        if bbox_value:
            if isinstance(bbox_value, str):
                parts = [part.strip() for part in bbox_value.split(",")[:4]]
            elif isinstance(bbox_value, (list, tuple)) and len(bbox_value) >= 4:
                parts = [str(part).strip() for part in bbox_value[:4]]
            else:
                parts = []
            if len(parts) == 4:
                west, south, east, north = [float(part) for part in parts]
                return _bbox_intersects_tile(west, south, east, north, x=x, y=y, z=z)
    except Exception:
        return True

    try:
        asset_zoom = asset.get("zoom")
        if asset_zoom is not None and int(asset_zoom) == int(z):
            min_x = int(asset.get("tile_min_x"))
            max_x = int(asset.get("tile_max_x"))
            min_y = int(asset.get("tile_min_y"))
            max_y = int(asset.get("tile_max_y"))
            return min_x <= x <= max_x and min_y <= y <= max_y
    except Exception:
        return True
    return True


def _lon_to_tile_x(lon: float, zoom: int) -> int:
    n = 2**zoom
    lon = max(-180.0, min(180.0, lon))
    return max(0, min(n - 1, int((lon + 180.0) / 360.0 * n)))


def _lat_to_tile_y(lat: float, zoom: int) -> int:
    import math

    n = 2**zoom
    lat = max(-85.05112878, min(85.05112878, lat))
    lat_rad = math.radians(lat)
    tile_y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, tile_y))


def _bbox_intersects_tile(west: float, south: float, east: float, north: float, *, x: int, y: int, z: int) -> bool:
    min_x = _lon_to_tile_x(west, z)
    max_x = _lon_to_tile_x(east, z)
    min_y = _lat_to_tile_y(north, z)
    max_y = _lat_to_tile_y(south, z)
    return min_x <= x <= max_x and min_y <= y <= max_y


def _tile_response(*, content: bytes, visual_empty: bool, metadata: dict[str, Any]) -> Response:
    headers = {
        "Cache-Control": "max-age=1800" if visual_empty else "max-age=7200",
        "x-agroclimax-product-kind": str(metadata.get("coverage_origin") or "object_storage"),
        "x-agroclimax-source-date": str(metadata.get("resolved_source_date") or ""),
        "x-agroclimax-visual-state": str(metadata.get("visual_state") or ("empty" if visual_empty else "ready")),
        "x-agroclimax-visual-empty": "1" if visual_empty else "0",
        "x-agroclimax-renderable-pct": str(metadata.get("renderable_pixel_pct") or (0.0 if visual_empty else 100.0)),
    }
    if metadata.get("empty_reason"):
        headers["x-agroclimax-empty-reason"] = str(metadata.get("empty_reason"))
    return Response(content=content, media_type="image/png", status_code=200, headers=headers)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tiles/{layer_id}/{z}/{x}/{y}.png")
async def tile_png(
    layer_id: str,
    z: int,
    x: int,
    y: int,
    display_date: date = Query(...),
    scope_type: str | None = Query(None),
    scope_ref: str | None = Query(None),
    department: str | None = Query(None),
    unit_id: str | None = Query(None),
):
    normalized_scope = str(scope_type or "").strip().lower()
    department_name = _canonical_department_name(department or (scope_ref if normalized_scope in {"departamento", "department"} else None))
    is_national = normalized_scope in {"nacional", "national"} or (not department_name and normalized_scope not in {"departamento", "department"})

    resolved_source_date = display_date.isoformat()

    if is_national:
        payload = await _load_mosaic_payload(layer_id, display_date)
        if payload is None:
            return JSONResponse({"detail": "mosaic_not_found"}, status_code=404)
        assets = list(payload.get("assets") or [])
        intersecting_assets = [asset for asset in assets if _asset_intersects_tile(asset, x=x, y=y, z=z)]
        if not intersecting_assets:
            return _tile_response(
                content=TRANSPARENT_TILE_PNG,
                visual_empty=True,
                metadata={
                    "coverage_origin": "national_mosaic",
                    "resolved_source_date": resolved_source_date,
                    "visual_state": "empty",
                    "visual_empty": True,
                    "renderable_pixel_pct": 0.0,
                    "empty_reason": "national_mosaic_tile_outside_assets",
                },
            )
        canvas = Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0))
        loaded_assets = 0
        rendered_assets = 0
        best_pct = 0.0
        for asset in intersecting_assets:
            storage_key = str(asset.get("storage_key") or "").strip()
            if not storage_key:
                continue
            content, meta = await _read_cog_tile(storage_key, x=x, y=y, z=z)
            if not content:
                continue
            try:
                with Image.open(io.BytesIO(content)) as image:
                    rgba = image.convert("RGBA")
                    canvas.alpha_composite(rgba)
                loaded_assets += 1
                current_pct = float(meta.get("renderable_pixel_pct") or 0.0)
                if current_pct > 0.0:
                    rendered_assets += 1
                best_pct = max(best_pct, current_pct)
            except Exception:
                continue
        if loaded_assets <= 0:
            # Not a true "empty": it's a missing/failed tile (missing assets, credentials, etc.).
            # Let the backend fall back to remote rendering instead of locking a false-empty tile.
            return JSONResponse({"detail": "national_mosaic_tile_unavailable"}, status_code=404)
        canvas_rgba = np.asarray(canvas, dtype=np.uint8)
        canvas_renderable_pct = _renderable_pct_from_rgba(canvas_rgba)
        if rendered_assets <= 0 or canvas_renderable_pct <= 0.0:
            return _tile_response(
                content=TRANSPARENT_TILE_PNG,
                visual_empty=True,
                metadata={
                    "coverage_origin": "national_mosaic",
                    "resolved_source_date": resolved_source_date,
                    "visual_state": "empty",
                    "visual_empty": True,
                    "renderable_pixel_pct": 0.0,
                    "empty_reason": "national_mosaic_tile_empty",
                },
            )
        buffer = io.BytesIO()
        canvas.save(buffer, format="PNG")
        return _tile_response(
            content=buffer.getvalue(),
            visual_empty=False,
            metadata={
                "coverage_origin": "national_mosaic",
                "resolved_source_date": resolved_source_date,
                "visual_state": "ready",
                "visual_empty": False,
                "renderable_pixel_pct": max(best_pct, canvas_renderable_pct),
            },
        )

    if not department_name:
        return JSONResponse({"detail": "department_required"}, status_code=400)

    storage_key = _department_raster_storage_key(layer_id=layer_id, display_date=display_date, department=department_name)
    content, meta = await _read_cog_tile(storage_key, x=x, y=y, z=z)
    if not content:
        return JSONResponse({"detail": "cog_not_found"}, status_code=404)
    try:
        renderable_pct = float(meta.get("renderable_pixel_pct") or 0.0)
    except Exception:
        renderable_pct = 0.0
    visual_empty = renderable_pct <= 0.0
    return _tile_response(
        content=TRANSPARENT_TILE_PNG if visual_empty else content,
        visual_empty=visual_empty,
        metadata={
            "coverage_origin": "department_daily_cog",
            "resolved_source_date": resolved_source_date,
            "visual_state": "empty" if visual_empty else "ready",
            "visual_empty": visual_empty,
            "renderable_pixel_pct": renderable_pct,
            **({"empty_reason": "department_tile_empty"} if visual_empty else {}),
        },
    )
