from __future__ import annotations

import base64
import io
from datetime import date

import httpx
from PIL import Image

from app.core.config import settings


_TRANSPARENT_PNG_1PX = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR42mP4DwQACfsD/Ql8Z9sAAAAASUVORK5CYII="
)


def _build_transparent_tile_png(size: int = 256) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGBA", (size, size), (0, 0, 0, 0)).save(buffer, format="PNG")
    return buffer.getvalue()


TRANSPARENT_PNG = _build_transparent_tile_png(256) or _TRANSPARENT_PNG_1PX


_HTTP_CLIENT: httpx.AsyncClient | None = None


def _tileserver_http_client() -> httpx.AsyncClient:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        timeout_seconds = max(float(settings.tileserver_request_timeout_seconds or 20.0), 1.0)
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
        )
    return _HTTP_CLIENT


async def fetch_tileserver_tile(
    *,
    layer_id: str,
    display_date: date,
    z: int,
    x: int,
    y: int,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> tuple[bytes | None, dict | None]:
    base_url = settings.tileserver_internal_url.strip().rstrip("/")
    if not base_url:
        return None, None
    params = {
        "display_date": display_date.isoformat(),
        **({"scope_type": scope_type} if scope_type else {}),
        **({"scope_ref": scope_ref} if scope_ref else {}),
        **({"department": department} if department else {}),
        **({"unit_id": unit_id} if unit_id else {}),
    }
    client = _tileserver_http_client()
    response = await client.get(
        f"{base_url}/tiles/{layer_id}/{z}/{x}/{y}.png",
        params=params,
    )
    if response.status_code == 404 and not response.headers.get("x-agroclimax-visual-empty"):
        return None, None
    response.raise_for_status()
    visual_empty = response.headers.get("x-agroclimax-visual-empty") == "1"
    metadata = {
        "coverage_origin": response.headers.get("x-agroclimax-product-kind") or "internal_tileserver",
        "resolved_source_date": response.headers.get("x-agroclimax-source-date"),
        "visual_state": response.headers.get("x-agroclimax-visual-state") or ("empty" if visual_empty else "ready"),
        "visual_empty": visual_empty,
        "renderable_pixel_pct": float(response.headers.get("x-agroclimax-renderable-pct") or 0.0),
        "cache_status": "empty" if visual_empty else "ready",
    }
    content = response.content or (TRANSPARENT_PNG if visual_empty else None)
    return content, metadata
