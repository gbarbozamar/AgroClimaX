from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from io import BytesIO
import math
from pathlib import Path
import secrets
from typing import Any
from urllib.parse import urlparse

from PIL import Image, ImageDraw, ImageFont
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, shape
from shapely.ops import unary_union
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.alerta import NotificationMediaAsset
from app.models.humedad import AOIUnit
from app.services.object_storage import storage_get_bytes, storage_put_bytes
from app.services.public_api import TRANSPARENT_PNG, fetch_tile_png


ASSET_DIR = Path(__file__).resolve().parents[2] / ".notification_assets"
ASSET_DIR.mkdir(exist_ok=True)
IMAGE_WIDTH = 1120
HEADER_HEIGHT = 116
FOOTER_HEIGHT = 56
TEXT_COLOR = "#e8edf8"
MUTED_TEXT = "#aab6d3"
PANEL_BG = "#111625"
STATE_COLORS = {
    "Normal": "#2ecc71",
    "Vigilancia": "#f1c40f",
    "Alerta": "#e67e22",
    "Emergencia": "#e74c3c",
}


def _font(size: int):
    try:
        return ImageFont.truetype("arial.ttf", size=size)
    except Exception:
        return ImageFont.load_default()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_geometry(geojson: dict[str, Any] | None):
    if not geojson:
        return None
    try:
        geometry = shape(geojson)
    except Exception:
        return None
    if geometry.is_empty:
        return None
    return geometry


def _bounds_with_padding(bounds: tuple[float, float, float, float], ratio: float = 0.16) -> tuple[float, float, float, float]:
    west, south, east, north = bounds
    width = max(east - west, 0.02)
    height = max(north - south, 0.02)
    pad_x = width * ratio
    pad_y = height * ratio
    return (
        max(-179.99, west - pad_x),
        max(-85.0, south - pad_y),
        min(179.99, east + pad_x),
        min(85.0, north + pad_y),
    )


def _lon_to_tile_x(lon: float, zoom: int) -> float:
    n = 2 ** zoom
    return ((lon + 180.0) / 360.0) * n


def _lat_to_tile_y(lat: float, zoom: int) -> float:
    lat = max(-85.05112878, min(85.05112878, lat))
    lat_rad = math.radians(lat)
    n = 2 ** zoom
    return ((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0) * n


def _pixel_xy(lon: float, lat: float, zoom: int) -> tuple[float, float]:
    return _lon_to_tile_x(lon, zoom) * 256.0, _lat_to_tile_y(lat, zoom) * 256.0


def _choose_zoom(scope_type: str, bounds: tuple[float, float, float, float]) -> int:
    width = abs(bounds[2] - bounds[0])
    height = abs(bounds[3] - bounds[1])
    span = max(width, height)
    if scope_type in {"productive_unit", "field"}:
        return 11 if span < 0.2 else 10
    if scope_type == "department":
        return 7 if span > 0.8 else 8
    return 5


async def _render_sar_background(scope_type: str, geometry_geojson: dict[str, Any] | None) -> tuple[Image.Image, dict[str, Any]]:
    geometry = _normalize_geometry(geometry_geojson)
    if geometry is None:
        canvas = Image.new("RGBA", (IMAGE_WIDTH, 720), PANEL_BG)
        return canvas, {"geometry": None, "zoom": 0, "origin_px": (0.0, 0.0)}

    padded = _bounds_with_padding(geometry.bounds)
    zoom = _choose_zoom(scope_type, padded)
    x_min = int(math.floor(_lon_to_tile_x(padded[0], zoom)))
    x_max = int(math.floor(_lon_to_tile_x(padded[2], zoom)))
    y_min = int(math.floor(_lat_to_tile_y(padded[3], zoom)))
    y_max = int(math.floor(_lat_to_tile_y(padded[1], zoom)))
    x_range = range(max(0, x_min), max(0, x_max) + 1)
    y_range = range(max(0, y_min), max(0, y_max) + 1)
    tile_width = max(1, len(list(x_range))) * 256
    tile_height = max(1, len(list(y_range))) * 256
    mosaic = Image.new("RGBA", (tile_width, tile_height), "#0e1423")

    tasks = []
    coords: list[tuple[int, int, int, int]] = []
    for yi, tile_y in enumerate(y_range):
        for xi, tile_x in enumerate(x_range):
            tasks.append(fetch_tile_png("sar", zoom, tile_x, tile_y))
            coords.append((tile_x, tile_y, xi * 256, yi * 256))
    tile_bytes_list = await asyncio.gather(*tasks, return_exceptions=True)
    for tile_bytes, (_, _, offset_x, offset_y) in zip(tile_bytes_list, coords):
        content = TRANSPARENT_PNG if isinstance(tile_bytes, Exception) else tile_bytes
        try:
            tile_image = Image.open(BytesIO(content)).convert("RGBA")
        except Exception:
            tile_image = Image.new("RGBA", (256, 256), "#0e1423")
        mosaic.alpha_composite(tile_image, (offset_x, offset_y))

    origin_px = (x_range.start * 256.0, y_range.start * 256.0)
    min_px = _pixel_xy(padded[0], padded[3], zoom)
    max_px = _pixel_xy(padded[2], padded[1], zoom)
    crop_box = (
        max(0, int(min_px[0] - origin_px[0])),
        max(0, int(min_px[1] - origin_px[1])),
        min(mosaic.width, int(math.ceil(max_px[0] - origin_px[0]))),
        min(mosaic.height, int(math.ceil(max_px[1] - origin_px[1]))),
    )
    cropped = mosaic.crop(crop_box)
    if cropped.width < 10 or cropped.height < 10:
        cropped = mosaic
    if cropped.width > IMAGE_WIDTH:
        target_height = max(360, int(cropped.height * (IMAGE_WIDTH / cropped.width)))
        cropped = cropped.resize((IMAGE_WIDTH, target_height), Image.Resampling.LANCZOS)

    return cropped, {"geometry": geometry, "zoom": zoom, "origin_px": origin_px, "crop_box": crop_box, "bounds": padded}


def _project_point(lon: float, lat: float, zoom: int, origin_px: tuple[float, float], crop_box: tuple[int, int, int, int], image_size: tuple[int, int]) -> tuple[float, float]:
    px, py = _pixel_xy(lon, lat, zoom)
    relative_x = px - origin_px[0] - crop_box[0]
    relative_y = py - origin_px[1] - crop_box[1]
    scale_x = image_size[0] / max(crop_box[2] - crop_box[0], 1)
    scale_y = image_size[1] / max(crop_box[3] - crop_box[1], 1)
    return relative_x * scale_x, relative_y * scale_y


def _draw_geometry_overlay(image: Image.Image, geometry, meta: dict[str, Any], *, fill_rgba: tuple[int, int, int, int], outline_rgba: tuple[int, int, int, int], width: int = 4) -> None:
    if geometry is None:
        return
    draw = ImageDraw.Draw(image, "RGBA")
    zoom = meta.get("zoom", 0)
    origin_px = meta.get("origin_px", (0.0, 0.0))
    crop_box = meta.get("crop_box", (0, 0, image.width, image.height))

    def _points_for_ring(ring):
        return [
            _project_point(lon, lat, zoom, origin_px, crop_box, image.size)
            for lon, lat in ring
        ]

    polygons: list[Polygon] = []
    if isinstance(geometry, Polygon):
        polygons = [geometry]
    elif isinstance(geometry, MultiPolygon):
        polygons = list(geometry.geoms)
    elif isinstance(geometry, GeometryCollection):
        polygons = [geom for geom in geometry.geoms if isinstance(geom, Polygon)]

    for polygon in polygons:
        exterior = _points_for_ring(list(polygon.exterior.coords))
        if len(exterior) >= 3:
            draw.polygon(exterior, fill=fill_rgba, outline=outline_rgba)
            draw.line(exterior + [exterior[0]], fill=outline_rgba, width=width)


def _draw_labeled_overlay_features(
    image: Image.Image,
    features: list[dict[str, Any]] | None,
    meta: dict[str, Any],
    *,
    outline_rgba: tuple[int, int, int, int] = (255, 255, 255, 230),
    label_fill: str = "#f5f7ff",
    width: int = 3,
) -> None:
    if not features:
        return
    draw = ImageDraw.Draw(image, "RGBA")
    font = _font(18)
    zoom = meta.get("zoom", 0)
    origin_px = meta.get("origin_px", (0.0, 0.0))
    crop_box = meta.get("crop_box", (0, 0, image.width, image.height))

    for feature in features:
        geometry_geojson = feature.get("geometry_geojson")
        label = str(feature.get("label") or "").strip()
        geometry = _normalize_geometry(geometry_geojson)
        if geometry is None:
            continue
        polygons: list[Polygon] = []
        if isinstance(geometry, Polygon):
            polygons = [geometry]
        elif isinstance(geometry, MultiPolygon):
            polygons = list(geometry.geoms)
        elif isinstance(geometry, GeometryCollection):
            polygons = [geom for geom in geometry.geoms if isinstance(geom, Polygon)]
        for polygon in polygons:
            exterior = [
                _project_point(lon, lat, zoom, origin_px, crop_box, image.size)
                for lon, lat in list(polygon.exterior.coords)
            ]
            if len(exterior) >= 3:
                draw.line(exterior + [exterior[0]], fill=outline_rgba, width=width)
        if label:
            point = geometry.representative_point()
            px, py = _project_point(point.x, point.y, zoom, origin_px, crop_box, image.size)
            text_width = int(draw.textlength(label, font=font))
            draw.rounded_rectangle(
                (px - 10, py - 14, px + text_width + 10, py + 18),
                radius=10,
                fill=(13, 19, 33, 210),
                outline=outline_rgba,
            )
            draw.text((px, py - 8), label, fill=label_fill, font=font)


def _compose_card(base: Image.Image, *, title: str, subtitle: str, badge: str, badge_color: str, lines: list[str]) -> Image.Image:
    card = Image.new("RGBA", (base.width, base.height + HEADER_HEIGHT + FOOTER_HEIGHT), PANEL_BG)
    card.alpha_composite(base, (0, HEADER_HEIGHT))
    draw = ImageDraw.Draw(card, "RGBA")

    draw.rectangle((0, 0, card.width, HEADER_HEIGHT), fill="#0d1321")
    draw.rectangle((0, card.height - FOOTER_HEIGHT, card.width, card.height), fill="#0d1321")

    title_font = _font(28)
    subtitle_font = _font(16)
    badge_font = _font(18)
    small_font = _font(14)

    draw.text((28, 20), title, fill=TEXT_COLOR, font=title_font)
    draw.text((28, 58), subtitle, fill=MUTED_TEXT, font=subtitle_font)
    badge_width = max(120, int(draw.textlength(badge, font=badge_font) + 28))
    draw.rounded_rectangle((card.width - badge_width - 26, 22, card.width - 26, 58), radius=16, fill=badge_color)
    draw.text((card.width - badge_width - 12, 31), badge, fill="#ffffff", font=badge_font)

    footer_x = 28
    for line in lines:
        draw.text((footer_x, card.height - FOOTER_HEIGHT + 18), line, fill=MUTED_TEXT, font=small_font)
        footer_x += int(draw.textlength(line, font=small_font)) + 24
    return card


def _png_bytes(image: Image.Image) -> bytes:
    buffer = BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def _local_asset_path(storage_key: str) -> Path:
    path = ASSET_DIR.joinpath(*[part for part in storage_key.split("/") if part])
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


async def _store_asset(storage_key: str, content: bytes, *, content_type: str = "image/png") -> None:
    path = _local_asset_path(storage_key)
    path.write_bytes(content)
    if settings.storage_bucket_enabled:
        await storage_put_bytes(storage_key, content, content_type=content_type)


async def load_media_asset_bytes(storage_key: str) -> tuple[bytes, str]:
    if settings.storage_bucket_enabled:
        bucket_value = await storage_get_bytes(storage_key)
        if bucket_value is not None:
            content, content_type, _ = bucket_value
            return content, content_type or "image/png"
    path = _local_asset_path(storage_key)
    return path.read_bytes(), "image/png"


def public_base_url() -> str:
    if settings.public_app_base_url:
        return settings.public_app_base_url.rstrip("/")
    if settings.google_redirect_uri:
        parsed = urlparse(settings.google_redirect_uri)
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return "http://127.0.0.1:8050"


def asset_public_url(asset: NotificationMediaAsset) -> str:
    return f"{public_base_url()}/api/v1/alert-subscriptions/assets/{asset.id}?token={asset.access_token}"


async def create_notification_media_assets(
    session: AsyncSession,
    *,
    scope_type: str,
    scope_id: str | None,
    scope_label: str,
    geometry_geojson: dict[str, Any] | None,
    state_name: str,
    observed_at: datetime | None,
    department: str | None,
    risk_score: float | None,
    confidence_score: float | None,
    affected_pct: float | None,
    overlay_features: list[dict[str, Any]] | None = None,
    alert_event_id: str | None = None,
    subscription_id: str | None = None,
) -> list[dict[str, Any]]:
    base_image, meta = await _render_sar_background(scope_type, geometry_geojson)
    geometry = meta.get("geometry")
    state_color = STATE_COLORS.get(state_name or "Normal", "#4a90d9")
    rgba_fill = tuple(int(state_color[i : i + 2], 16) for i in (1, 3, 5)) + (82,)
    rgba_outline = tuple(int(state_color[i : i + 2], 16) for i in (1, 3, 5)) + (255,)

    overview_background = base_image.copy()
    overlay = Image.new("RGBA", overview_background.size, (0, 0, 0, 0))
    _draw_geometry_overlay(overlay, geometry, meta, fill_rgba=rgba_fill, outline_rgba=rgba_outline, width=5)
    overview_background = Image.alpha_composite(overview_background, overlay)
    _draw_labeled_overlay_features(overview_background, overlay_features, meta)
    overview_card = _compose_card(
        overview_background,
        title=f"{scope_label} | alerta {state_name or 'Normal'}",
        subtitle=f"{department or 'Uruguay'} · {scope_type} · {observed_at.date().isoformat() if observed_at else 'sin fecha'}",
        badge=state_name or "Normal",
        badge_color=state_color,
        lines=[
            f"Risk {round(float(risk_score or 0.0), 1)}",
            f"Confianza {round(float(confidence_score or 0.0), 1)}%",
            f"Area afectada {round(float(affected_pct or 0.0), 1)}%",
        ],
    )

    humidity_background = base_image.copy()
    humidity_overlay = Image.new("RGBA", humidity_background.size, (0, 0, 0, 0))
    _draw_geometry_overlay(humidity_overlay, geometry, meta, fill_rgba=(53, 207, 131, 26), outline_rgba=(141, 255, 194, 255), width=4)
    humidity_background = Image.alpha_composite(humidity_background, humidity_overlay)
    _draw_labeled_overlay_features(
        humidity_background,
        overlay_features,
        meta,
        outline_rgba=(173, 234, 206, 220),
        label_fill="#d8fff0",
        width=2,
    )
    humidity_card = _compose_card(
        humidity_background,
        title=f"{scope_label} | Humedad Superficial del Suelo",
        subtitle=f"Sentinel-1 SAR · capa de humedad superficial · {department or 'Uruguay'}",
        badge="SAR",
        badge_color="#1f78d1",
        lines=[
            f"Scope {scope_type}",
            f"Observado {observed_at.date().isoformat() if observed_at else 'N/D'}",
            "Fuente Sentinel-1",
        ],
    )

    results: list[dict[str, Any]] = []
    timestamp = int(_now_utc().timestamp())
    for kind, image in (("alert_overview", overview_card), ("surface_soil_moisture", humidity_card)):
        content = _png_bytes(image)
        asset = NotificationMediaAsset(
            alert_event_id=alert_event_id,
            subscription_id=subscription_id,
            scope_type=scope_type,
            scope_id=scope_id,
            kind=kind,
            mime_type="image/png",
            storage_key=f"notification_media/{scope_type}/{scope_id or 'global'}/{timestamp}-{secrets.token_hex(8)}-{kind}.png",
            access_token=secrets.token_urlsafe(24),
            width=image.width,
            height=image.height,
            metadata_extra={"scope_label": scope_label, "department": department or "Uruguay"},
        )
        session.add(asset)
        await session.flush()
        await _store_asset(asset.storage_key, content, content_type="image/png")
        results.append(
            {
                "id": asset.id,
                "kind": asset.kind,
                "mime_type": asset.mime_type,
                "storage_key": asset.storage_key,
                "width": asset.width,
                "height": asset.height,
                "url": asset_public_url(asset),
            }
        )
    return results


async def get_notification_media_asset(
    session: AsyncSession,
    *,
    asset_id: str,
    token: str,
) -> tuple[NotificationMediaAsset, bytes]:
    result = await session.execute(select(NotificationMediaAsset).where(NotificationMediaAsset.id == asset_id))
    asset = result.scalar_one_or_none()
    if asset is None or asset.access_token != token:
        raise ValueError("Asset no encontrado")
    content, _ = await load_media_asset_bytes(asset.storage_key)
    return asset, content


async def build_national_geometry(session: AsyncSession) -> dict[str, Any] | None:
    result = await session.execute(
        select(AOIUnit.geometry_geojson).where(AOIUnit.unit_type == "department", AOIUnit.active.is_(True))
    )
    geometries = [_normalize_geometry(row[0]) for row in result.all() if row[0]]
    geometries = [geom for geom in geometries if geom is not None]
    if not geometries:
        return None
    merged = unary_union(geometries)
    return merged.__geo_interface__
