from datetime import date

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.analysis import get_timeline_context
from app.services.catalog import department_payloads
from app.services.preload import (
    get_preload_status,
    start_startup_preload,
    start_timeline_window_preload,
    start_viewport_preload,
)
from app.services.auth import AuthContext, try_auth_context
from app.services.public_api import (
    TRANSPARENT_PNG,
    build_timeline_frame_manifest,
    fetch_rivera_geojson,
    fetch_tile_png,
    list_official_map_overlays,
    proxy_coneat_request,
    proxy_official_overlay_tile,
)

router = APIRouter(tags=["public"])

# Router sin guard de sesión a nivel router. Sirve endpoints que pueden ser
# invocados por `<img>` tags del mapa (Leaflet), los cuales NO envían cookies
# por defecto. El endpoint /tiles sigue soportando `clip_scope=field` cuando
# la sesión sí viaja (p.ej. fetch same-origin), y degrada a sin-clip si no hay
# auth (ver handler).
public_router = APIRouter(tags=["public"])


class PreloadRequest(BaseModel):
    bbox: str | None = None
    zoom: int | None = None
    width: int = Field(default=1024, ge=128, le=4096)
    height: int = Field(default=768, ge=128, le=4096)
    temporal_layers: list[str] = Field(default_factory=list)
    official_layers: list[str] = Field(default_factory=list)
    scope_type: str | None = None
    scope_ref: str | None = None
    timeline_scope: str = "nacional"
    timeline_unit_id: str | None = None
    timeline_department: str | None = None
    target_date: date | None = None
    history_days: int = Field(default=30, ge=1, le=365)


class TimelineWindowPreloadRequest(PreloadRequest):
    date_from: date
    date_to: date


@router.get("/catalog/departamentos")
async def catalog_departamentos():
    return {"datos": department_payloads()}


@router.get("/geojson/rivera")
async def geojson_rivera():
    return await fetch_rivera_geojson()


@router.get("/proxy/coneat")
async def proxy_coneat(request: Request):
    content, content_type = await proxy_coneat_request(dict(request.query_params))
    return Response(content=content, media_type=content_type, headers={"Cache-Control": "max-age=86400"})


@router.get("/map-overlays/catalog")
async def map_overlay_catalog():
    return {"items": list_official_map_overlays()}


@router.get("/map-overlays/{overlay_id}/tile")
async def map_overlay_tile(
    overlay_id: str,
    bbox: str,
    bboxSR: str = "4326",
    imageSR: str = "4326",
    width: int = 256,
    height: int = 256,
    format: str = "image/png",
    transparent: bool = True,
):
    content, content_type = await proxy_official_overlay_tile(
        overlay_id,
        {
            "bbox": bbox,
            "bboxSR": bboxSR,
            "imageSR": imageSR,
            "width": width,
            "height": height,
            "format": format,
            "transparent": str(transparent).lower(),
        },
    )
    return Response(content=content, media_type=content_type, headers={"Cache-Control": "max-age=86400"})


@router.get("/timeline/frames")
async def timeline_frames(
    layers: list[str] = Query(...),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    bbox: str | None = Query(None),
    zoom: int | None = Query(None),
):
    return await build_timeline_frame_manifest(
        layers=layers,
        date_from=date_from,
        date_to=date_to,
        bbox=bbox,
        zoom=zoom,
    )


@router.get("/timeline/context")
async def timeline_context(
    scope: str = Query(...),
    unit_id: str | None = Query(None),
    department: str | None = Query(None),
    target_date: date = Query(...),
    history_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
):
    return await get_timeline_context(
        db,
        scope=scope,
        unit_id=unit_id,
        department=department,
        target_date=target_date,
        history_days=history_days,
    )


@router.post("/preload/startup")
async def preload_startup(payload: PreloadRequest):
    return await start_startup_preload(
        bbox=payload.bbox,
        zoom=payload.zoom,
        width=payload.width,
        height=payload.height,
        temporal_layers=payload.temporal_layers,
        official_layers=payload.official_layers,
        scope_type=payload.scope_type,
        scope_ref=payload.scope_ref,
        timeline_scope=payload.timeline_scope,
        timeline_unit_id=payload.timeline_unit_id,
        timeline_department=payload.timeline_department,
        target_date=payload.target_date,
        history_days=payload.history_days,
    )


@router.post("/preload/viewport")
async def preload_viewport(payload: PreloadRequest):
    return await start_viewport_preload(
        bbox=payload.bbox,
        zoom=payload.zoom,
        width=payload.width,
        height=payload.height,
        temporal_layers=payload.temporal_layers,
        official_layers=payload.official_layers,
        scope_type=payload.scope_type,
        scope_ref=payload.scope_ref,
        timeline_scope=payload.timeline_scope,
        timeline_unit_id=payload.timeline_unit_id,
        timeline_department=payload.timeline_department,
        target_date=payload.target_date,
        history_days=payload.history_days,
    )


@router.post("/preload/timeline-window")
async def preload_timeline_window(payload: TimelineWindowPreloadRequest):
    return await start_timeline_window_preload(
        bbox=payload.bbox,
        zoom=payload.zoom,
        width=payload.width,
        height=payload.height,
        temporal_layers=payload.temporal_layers,
        scope_type=payload.scope_type,
        scope_ref=payload.scope_ref,
        timeline_scope=payload.timeline_scope,
        timeline_unit_id=payload.timeline_unit_id,
        timeline_department=payload.timeline_department,
        date_from=payload.date_from,
        date_to=payload.date_to,
        history_days=payload.history_days,
    )


@router.get("/preload/status")
async def preload_status(run_key: str = Query(...)):
    return await get_preload_status(run_key)


@public_router.get("/tiles/{layer}/{z}/{x}/{y}.png")
async def tiles(
    layer: str,
    z: int,
    x: int,
    y: int,
    source_date: date | None = Query(None),
    frame_role: str | None = Query(None),
    clip_scope: str | None = Query(None),
    clip_ref: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    auth: AuthContext | None = Depends(try_auth_context),
):
    """
    Si `clip_scope` está presente, el tile se recorta al polígono resuelto.
    Tiles disjuntos de la geometría devuelven transparente sin pegar a
    Copernicus ni cachear (early exit).

    `clip_scope='field'` sólo es válido con sesión autenticada; el ownership
    check ocurre dentro de `fetch_tile_png` → `resolve_scope_geometry`.

    Si la request viene sin sesión (p.ej. `<img>` tag de Leaflet que no envía
    cookies cross-fetch), degradamos a `clip_scope=None` en vez de responder
    401: el frontend tiene un visual clipMask por encima que oculta el área
    exterior al potrero, así que servir el tile base sin recorte server-side
    no filtra datos privados y evita romper el mapa por completo.
    """
    user_id = auth.user.id if (auth and auth.user) else None
    if clip_scope == "field" and user_id is None:
        clip_scope = None
        clip_ref = None
    image = await fetch_tile_png(
        layer, z, x, y,
        target_date=source_date,
        frame_role=frame_role,
        clip_scope=clip_scope,
        clip_ref=clip_ref,
        db=db,
        user_id=user_id,
    )
    return Response(content=image or TRANSPARENT_PNG, media_type="image/png", headers={"Cache-Control": "max-age=7200"})
