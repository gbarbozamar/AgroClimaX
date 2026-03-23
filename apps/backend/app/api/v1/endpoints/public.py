from fastapi import APIRouter, Request
from fastapi.responses import Response

from app.services.catalog import department_payloads
from app.services.public_api import TRANSPARENT_PNG, fetch_rivera_geojson, fetch_tile_png, proxy_coneat_request

router = APIRouter(tags=["public"])


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


@router.get("/tiles/{layer}/{z}/{x}/{y}.png")
async def tiles(layer: str, z: int, x: int, y: int):
    image = await fetch_tile_png(layer, z, x, y)
    return Response(content=image or TRANSPARENT_PNG, media_type="image/png", headers={"Cache-Control": "max-age=7200"})
