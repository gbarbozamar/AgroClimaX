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
    canvas = Image.new("RGBA", (native_w, native_h), (0, 0, 0, 0))

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
            if not png_bytes or len(png_bytes) <= 100:
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

    payload = {
        "field_id": field_id,
        "user_id": user_id,
        "layer_key": layer_key,
        "observed_at": observed_col,
        # Compat con ambos nombres de columna usados por agentes paralelos.
        "storage_path": str(out_path),
        "storage_key": str(out_path),
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
