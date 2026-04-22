"""Servicio para generar snapshots PNG de farm fields.

Toma un farm field + layer_key + fecha, compone los XYZ tiles que cubren su
bbox en un único PNG proporcional, y persiste el resultado como
FieldImageSnapshot (unique por field+layer+observed_at).

Depende de:
    * app.models.field_snapshot.FieldImageSnapshot (otro agente materializa)
    * app.services.aoi_tile_clip.resolve_scope_geometry
    * app.services.public_api.fetch_tile_png
"""

from __future__ import annotations

import io
import logging
import math
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.services import aoi_tile_clip, public_api
from app.services.aoi_tile_clip import resolve_scope_geometry
from app.services.public_api import fetch_tile_png

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Root donde guardamos los PNG composites. Separado de public_api.TILE_CACHE_DIR
# (que guarda tiles individuales) para evitar mezclar granularidades.
SNAPSHOTS_ROOT = Path(__file__).resolve().parents[2] / ".tile_cache" / "fields"

# Pixeles por tile Sentinel Hub (public_api usa 256 en el request).
TILE_PX = 256

# Cap de tiles totales. Un zoom alto con bbox grande puede explotar — subimos
# de zoom 15 hacia 14 o 13 hasta caer bajo este cap.
MAX_TILES = 16
DEFAULT_ZOOM = 15
MIN_ZOOM = 13

# Canvas máximo en píxeles. Bbox chico puede quedar bajo esto, bbox grande se
# resamplea para no pasar 1024 de ancho.
MAX_WIDTH_PX = 1024


def _tile_count_for_zoom(bounds: tuple[float, float, float, float], zoom: int) -> int:
    xs, ys = public_api._tile_ranges_for_bounds(bounds, zoom)
    return len(xs) * len(ys)


def _pick_zoom(bounds: tuple[float, float, float, float]) -> int:
    """Elegir el mayor zoom en [MIN_ZOOM, DEFAULT_ZOOM] que quepa bajo MAX_TILES."""
    for z in range(DEFAULT_ZOOM, MIN_ZOOM - 1, -1):
        if _tile_count_for_zoom(bounds, z) <= MAX_TILES:
            return z
    return MIN_ZOOM


def _approx_area_ha(geom) -> float:
    """Aproximación ortoplanar grosera: útil como metadata, no para billing.

    Asume ~111 km por grado de lat y de lon (válido cerca del ecuador). Para
    latitudes medias sobreestima algo, pero nos alcanza como hint visual.
    """
    try:
        # geom.area viene en (deg)^2 porque las coords son lon/lat en EPSG:4326.
        return float(geom.area) * 111_000.0 * 111_000.0 / 10_000.0
    except Exception:
        return 0.0


async def _load_unit_index_snapshot(db, field, observed_at: date):
    """Leer UnitIndexSnapshot más cercano en fecha para el aoi_unit del field."""
    if not getattr(field, "aoi_unit_id", None):
        return None
    try:
        from app.models.materialized import UnitIndexSnapshot
    except Exception:  # pragma: no cover - model missing
        return None
    try:
        result = await db.execute(
            select(UnitIndexSnapshot)
            .where(UnitIndexSnapshot.unit_id == field.aoi_unit_id)
            .order_by(UnitIndexSnapshot.observed_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()
    except Exception as exc:
        logger.debug("UnitIndexSnapshot lookup failed field=%s exc=%s", field.id, exc)
        return None


def _pixel_bbox_for_tile_in_canvas(
    z: int,
    tx: int,
    ty: int,
    x_range: range,
    y_range: range,
    canvas_w: int,
    canvas_h: int,
) -> tuple[int, int]:
    """Paste-coords (top-left) para un tile dentro del canvas compuesto."""
    n_x = len(x_range)
    n_y = len(y_range)
    tile_w = canvas_w // n_x
    tile_h = canvas_h // n_y
    col = tx - x_range.start
    row = ty - y_range.start
    return col * tile_w, row * tile_h, tile_w, tile_h


async def render_field_snapshot(
    db: "AsyncSession",
    field_id: str,
    layer_key: str,
    observed_at: date,
    user_id: str | None = None,
):
    """Render y persiste el PNG composite de un field para (layer, fecha).

    Retorna la fila FieldImageSnapshot upsertada, o None si no hay geometry,
    no hay tiles válidos (todos transparent/67b), o el import del modelo falla.

    Si `user_id` es None, intenta derivar el owner del FarmField (para
    workflows de backfill/pipeline donde no hay sesión HTTP).
    """
    # 1. Geometría del field. Si el caller no pasó user_id, intentamos
    # derivarlo del FarmField row para bypassear el ownership check.
    resolved_user_id = user_id
    if resolved_user_id is None:
        try:
            from app.models.farm import FarmField
            row = (await db.execute(
                select(FarmField.user_id).where(FarmField.id == field_id).limit(1)
            )).first()
            if row:
                resolved_user_id = row[0]
        except Exception:
            pass
    try:
        geom = await resolve_scope_geometry(
            db, "field", field_id, user_id=resolved_user_id
        )
    except Exception as exc:
        logger.info(
            "render_field_snapshot: resolve_scope_geometry failed field=%s exc=%s",
            field_id, exc,
        )
        return None
    if geom is None or geom.is_empty:
        return None

    # 2. Bbox [W, S, E, N].
    try:
        west, south, east, north = geom.bounds
    except Exception:
        return None
    bounds: tuple[float, float, float, float] = (west, south, east, north)

    # 3-4. Tiles a zoom elegido bajo MAX_TILES.
    zoom = _pick_zoom(bounds)
    x_range, y_range = public_api._tile_ranges_for_bounds(bounds, zoom)
    n_x = len(x_range)
    n_y = len(y_range)
    if n_x == 0 or n_y == 0:
        return None

    # Load PIL acá para mantener la import cost fuera del path feliz del módulo.
    from PIL import Image  # type: ignore

    # Canvas nativo: n_x * 256 x n_y * 256. Downscale al final si pasa MAX_WIDTH_PX.
    native_w = n_x * TILE_PX
    native_h = n_y * TILE_PX
    # Fondo blanco sólido (antes era transparente → se veía negro en viewers).
    canvas = Image.new("RGBA", (native_w, native_h), (255, 255, 255, 255))

    # Geographic bounds REALES del canvas (tile edges, no field bbox).
    # Esto es crítico para que los paddock boundaries queden alineados con la
    # imagen: los XYZ tiles cubren una zona mayor que el field bbox (tiles son
    # discretos), y el canvas pinta esa zona completa.
    n_tiles = 2 ** zoom
    canvas_west = x_range.start * 360.0 / n_tiles - 180.0
    canvas_east = (x_range.start + n_x) * 360.0 / n_tiles - 180.0
    _north_rad = math.atan(math.sinh(math.pi - 2 * math.pi * y_range.start / n_tiles))
    _south_rad = math.atan(math.sinh(math.pi - 2 * math.pi * (y_range.start + n_y) / n_tiles))
    canvas_north = math.degrees(_north_rad)
    canvas_south = math.degrees(_south_rad)

    valid_tiles = 0
    for tx in x_range:
        for ty in y_range:
            png_bytes = await fetch_tile_png(
                layer_key,
                zoom,
                tx,
                ty,
                target_date=observed_at,
                clip_scope="field",
                clip_ref=field_id,
                db=db,
                user_id=resolved_user_id,
            )
            # Skip 67b transparent placeholders y cualquier dato inválido.
            # Umbral alineado con public_api._MIN_VALID_TILE_BYTES (500) para
            # rechazar también tiles erróneos de Copernicus (~334 bytes).
            if not png_bytes or len(png_bytes) < 500:
                continue
            try:
                tile_img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
            except Exception as exc:
                logger.debug(
                    "tile decode failed layer=%s z=%s x=%s y=%s exc=%s",
                    layer_key, zoom, tx, ty, exc,
                )
                continue
            px, py, tw, th = _pixel_bbox_for_tile_in_canvas(
                zoom, tx, ty, x_range, y_range, native_w, native_h,
            )
            # tile_img siempre 256x256 en este pipeline; resize defensivo.
            if tile_img.size != (tw, th):
                tile_img = tile_img.resize((tw, th))
            canvas.paste(tile_img, (px, py), tile_img)
            valid_tiles += 1

    if valid_tiles == 0:
        return None

    # 5a. Clippear canvas al polígono del field: los tiles XYZ cubren un área
    # RECTANGULAR mayor que el field real; queremos que la imagen visible sea
    # solo el field (ni tiles vecinos, ni bordes del rectángulo de tiles).
    # Pixeles fuera del field → blancos. Esto además alinea perfectamente los
    # paddocks que se dibujan después, porque el "área con imagen" = field.
    try:
        from PIL import Image as _Image, ImageDraw as _ImageDraw
        mask = _Image.new("L", (native_w, native_h), 0)
        mdraw = _ImageDraw.Draw(mask)

        def _canvas_px(lon, lat):
            dx_ = canvas_east - canvas_west
            dy_ = canvas_north - canvas_south
            fx_ = (lon - canvas_west) / dx_ if dx_ > 0 else 0
            fy_ = (canvas_north - lat) / dy_ if dy_ > 0 else 0
            return (int(fx_ * native_w), int(fy_ * native_h))

        def _fill_polygon(poly):
            if poly.is_empty:
                return
            exterior = [_canvas_px(x, y) for x, y in poly.exterior.coords]
            if len(exterior) >= 3:
                mdraw.polygon(exterior, fill=255)
            for interior in poly.interiors:
                ring = [_canvas_px(x, y) for x, y in interior.coords]
                if len(ring) >= 3:
                    mdraw.polygon(ring, fill=0)

        if geom.geom_type == "Polygon":
            _fill_polygon(geom)
        elif geom.geom_type == "MultiPolygon":
            for sub in geom.geoms:
                _fill_polygon(sub)

        # Crear base blanca y pegar el canvas usando la mask del field.
        base = _Image.new("RGBA", (native_w, native_h), (255, 255, 255, 255))
        base.paste(canvas, (0, 0), mask)
        canvas = base
    except Exception as exc:
        logger.info("field mask clipping skipped: %s", exc)

    # 5b. Dibujar bordes de potreros (FarmPaddock) sobre el canvas.
    try:
        from app.models.farm import FarmPaddock
        from shapely.geometry import shape as _shape
        from PIL import ImageDraw
        paddock_rows = (await db.execute(
            select(FarmPaddock).where(
                FarmPaddock.field_id == field_id,
                FarmPaddock.active == True,  # noqa: E712
            )
        )).scalars().all()
        draw = ImageDraw.Draw(canvas, 'RGBA')
        # 5b.0 Dibujar el borde del field (naranja) para que el usuario vea la
        # silueta completa del campo aunque no haya datos Copernicus adentro.
        def _field_px(lon, lat):
            dx_ = canvas_east - canvas_west
            dy_ = canvas_north - canvas_south
            fx_ = (lon - canvas_west) / dx_ if dx_ > 0 else 0
            fy_ = (canvas_north - lat) / dy_ if dy_ > 0 else 0
            return (int(fx_ * native_w), int(fy_ * native_h))

        def _draw_poly_outline(poly_, color, width):
            if poly_.is_empty:
                return
            ext = [_field_px(x, y) for x, y in poly_.exterior.coords]
            if len(ext) >= 2:
                draw.line(ext + [ext[0]], fill=color, width=width)

        if geom.geom_type == "Polygon":
            _draw_poly_outline(geom, (255, 140, 0, 240), 3)
        elif geom.geom_type == "MultiPolygon":
            for sub in geom.geoms:
                _draw_poly_outline(sub, (255, 140, 0, 240), 3)

        if paddock_rows:
            for p in paddock_rows:
                if not p.geometry_geojson:
                    continue
                try:
                    geom_p = _shape(p.geometry_geojson)
                except Exception:
                    continue
                # Convertir coords lat/lng a pixel coords dentro del canvas.
                # IMPORTANTE: el canvas cubre (canvas_west..canvas_east, canvas_south..canvas_north)
                # que son los bordes de los XYZ tiles — NO los bounds del field.
                # Si usáramos field bounds (west/east/south/north) los paddocks
                # se dibujarían estirados y más grandes que la imagen real.
                def lonlat_to_px(lon, lat):
                    dx = canvas_east - canvas_west
                    dy = canvas_north - canvas_south
                    fx = (lon - canvas_west) / dx if dx > 0 else 0
                    fy = (canvas_north - lat) / dy if dy > 0 else 0
                    return (int(fx * native_w), int(fy * native_h))
                # Iterar por geometrías MultiPolygon / Polygon / LineString.
                polys = []
                if geom_p.geom_type == 'Polygon':
                    polys.append(geom_p)
                elif geom_p.geom_type == 'MultiPolygon':
                    polys.extend(list(geom_p.geoms))
                for poly in polys:
                    coords = list(poly.exterior.coords)
                    px = [lonlat_to_px(lon, lat) for lon, lat in coords]
                    if len(px) >= 2:
                        # Línea amarilla semitransparente, grosor 3.
                        draw.line(px + [px[0]], fill=(255, 220, 0, 220), width=3)
    except Exception as exc:
        logger.info("paddock boundaries overlay skipped: %s", exc)

    # 5c. Etiqueta de fecha+layer+field_name en bottom-left del canvas.
    try:
        from PIL import ImageDraw, ImageFont
        from app.models.farm import FarmField
        field_row = (await db.execute(
            select(FarmField.name).where(FarmField.id == field_id).limit(1)
        )).first()
        field_name = field_row[0] if field_row else field_id[:8]
        label_text = f"{field_name}  |  {layer_key.upper()}  |  {observed_at.isoformat()}"
        draw = ImageDraw.Draw(canvas, 'RGBA')
        try:
            # Fuente default de PIL escalada por tamaño del canvas.
            font_size = max(14, native_w // 50)
            font = ImageFont.load_default(size=font_size)
        except Exception:
            font = ImageFont.load_default()
        # Box semitransparente detrás del texto para legibilidad.
        padding = 8
        bbox = draw.textbbox((0, 0), label_text, font=font)
        tw_px = bbox[2] - bbox[0]
        th_px = bbox[3] - bbox[1]
        margin = 12
        box_x0 = margin
        box_y0 = native_h - th_px - padding * 2 - margin
        box_x1 = box_x0 + tw_px + padding * 2
        box_y1 = native_h - margin
        # Fondo blanco semi-opaco + borde negro sutil + texto negro (legible
        # tanto sobre imágenes claras como oscuras).
        draw.rectangle(
            [(box_x0, box_y0), (box_x1, box_y1)],
            fill=(255, 255, 255, 230),
            outline=(0, 0, 0, 180),
            width=1,
        )
        draw.text(
            (box_x0 + padding, box_y0 + padding),
            label_text,
            fill=(20, 20, 20, 255),
            font=font,
        )
    except Exception as exc:
        logger.info("label overlay skipped: %s", exc)

    # 6. Downscale si excede cap de ancho.
    out_w, out_h = native_w, native_h
    if native_w > MAX_WIDTH_PX:
        scale = MAX_WIDTH_PX / native_w
        out_w = MAX_WIDTH_PX
        out_h = max(1, int(round(native_h * scale)))
        canvas = canvas.resize((out_w, out_h))

    buf = io.BytesIO()
    canvas.save(buf, format="PNG", optimize=True)
    png_payload = buf.getvalue()

    # 7. Area aprox.
    area_ha = _approx_area_ha(geom)

    # 8. Metadata del index más reciente del unit asociado, si existe.
    from app.models.farm import FarmField
    field_row = await db.execute(
        select(FarmField).where(FarmField.id == field_id).limit(1)
    )
    field_obj = field_row.scalar_one_or_none()
    index_snapshot = None
    if field_obj is not None:
        index_snapshot = await _load_unit_index_snapshot(db, field_obj, observed_at)

    risk_score = getattr(index_snapshot, "risk_score", None) if index_snapshot else None
    ndmi_mean = getattr(index_snapshot, "s2_ndmi_mean", None) if index_snapshot else None
    state = getattr(index_snapshot, "state", None) if index_snapshot else None

    # 9. Escribir PNG al filesystem. Usamos filesystem directo porque
    # storage_put_bytes apunta a S3 (retorna False si no hay bucket) y lo que
    # queremos acá es que esté siempre disponible localmente vía la ruta
    # absoluta que guardamos en FieldImageSnapshot.storage_path.
    out_dir = SNAPSHOTS_ROOT / field_id / "snapshots" / layer_key
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{observed_at.isoformat()}.png"
    try:
        out_path.write_bytes(png_payload)
    except OSError as exc:
        logger.warning("snapshot write failed path=%s exc=%s", out_path, exc)
        return None

    # Validar que el PNG persistido tiene contenido útil, no solo bordes amarillos
    # sobre fondo transparente (caso: todos los tiles fueron placeholders pero
    # valid_tiles > 0 porque >100 bytes es un threshold muy bajo).
    if len(png_payload) < 1200:
        logger.info(
            "render: PNG final muy chico (%d b), probable placeholder agregado",
            len(png_payload),
        )
        try:
            out_path.unlink()
        except Exception:
            pass
        return None

    # 10. Upsert FieldImageSnapshot. Si el modelo aún no fue materializado
    # por el otro agente, retornamos None sin romper.
    try:
        from app.models.field_snapshot import FieldImageSnapshot  # type: ignore
    except Exception as exc:
        logger.info(
            "FieldImageSnapshot model no disponible aún (%s); PNG guardado en %s",
            exc, out_path,
        )
        return None

    # El modelo declara observed_at como Date (no DateTime). Pasamos la `date`
    # directamente — convertir a datetime hacía que el query de idempotencia
    # no matchee al re-render con misma fecha (UNIQUE fail en INSERT).
    observed_col = observed_at

    existing_result = await db.execute(
        select(FieldImageSnapshot).where(
            FieldImageSnapshot.field_id == field_id,
            FieldImageSnapshot.layer_key == layer_key,
            FieldImageSnapshot.observed_at == observed_col,
        ).limit(1)
    )
    snapshot = existing_result.scalar_one_or_none()

    # user_id requerido por FK del modelo. Derivado del FarmField; fallback
    # a 'pipeline-anonymous' si el row no está en DB (caso edge del worker
    # que renderea desde geometría externa).
    user_id = (
        getattr(field_obj, "user_id", None)
        if field_obj
        else None
    ) or "pipeline-anonymous"

    # storage_key RELATIVO (fields/{field_id}/snapshots/{layer}/{date}.png)
    # es el contract que espera el endpoint GET /campos/{id}/snapshots/{key:path}.
    # Si usamos str(out_path) (absoluto Windows con backslashes) el endpoint no
    # puede servir el archivo — los thumbnails del slider quedan rotos.
    storage_key_rel = f"fields/{field_id}/snapshots/{layer_key}/{observed_at.isoformat()}.png"
    payload = {
        "field_id": field_id,
        "user_id": user_id,
        "layer_key": layer_key,
        "observed_at": observed_col,
        # storage_path guarda el path absoluto (uso interno del worker de video
        # que lee el archivo). storage_key es relativo para que el endpoint HTTP
        # lo pueda servir sin duplicar field_id en la URL.
        "storage_path": str(out_path),
        "storage_key": storage_key_rel,
        "width_px": out_w,
        "height_px": out_h,
        "bbox_json": [west, south, east, north],
        "bbox_west": west,
        "bbox_south": south,
        "bbox_east": east,
        "bbox_north": north,
        "zoom": zoom,
        "tiles_used": valid_tiles,
        "area_ha": area_ha,
        "risk_score": risk_score,
        "ndmi_mean": ndmi_mean,
        "s2_ndmi_mean": ndmi_mean,
        "state": state,
    }

    if snapshot is None:
        # Solo seteamos atributos que existan en el modelo para tolerar
        # variaciones de schema del otro agente.
        snapshot = FieldImageSnapshot()
        for k, v in payload.items():
            if hasattr(snapshot, k):
                setattr(snapshot, k, v)
        db.add(snapshot)
    else:
        for k, v in payload.items():
            if k in ("field_id", "layer_key", "observed_at"):
                continue
            if hasattr(snapshot, k):
                setattr(snapshot, k, v)

    try:
        await db.flush()
    except Exception as exc:
        logger.warning("FieldImageSnapshot flush failed field=%s exc=%s", field_id, exc)
        return None

    return snapshot


__all__ = ["render_field_snapshot"]
