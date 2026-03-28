from __future__ import annotations

import asyncio
import hashlib
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import httpx
import requests
from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.humedad import AOIUnit
from app.models.materialized import ExternalMapCacheEntry
from app.services.object_storage import storage_get_bytes, storage_put_bytes

try:
    from data_fetcher import get_token as legacy_get_token
except Exception:  # pragma: no cover
    legacy_get_token = None


SH_PROCESS_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"
TILE_CACHE_DIR = Path(__file__).resolve().parents[2] / ".tile_cache"
TILE_CACHE_DIR.mkdir(exist_ok=True)
CONEAT_CACHE_DIR = Path(__file__).resolve().parents[2] / ".coneat_cache"
CONEAT_CACHE_DIR.mkdir(exist_ok=True)
GADM_RIVERA_CACHE = Path(__file__).resolve().parents[2] / ".geojson_rivera.json"
GADM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_URY_1.json"
CONEAT_PROXY_SEMAPHORE = asyncio.Semaphore(2)
CONEAT_PROXY_RETRY_DELAYS = (0.45, 1.2, 2.4)

TRANSPARENT_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0bIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00"
    b"\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
)

CAPAS_INFO = {
    "rgb": {"src": "sentinel-2-l2a", "clouds": True},
    "ndvi": {"src": "sentinel-2-l2a", "clouds": True},
    "ndmi": {"src": "sentinel-2-l2a", "clouds": True},
    "ndwi": {"src": "sentinel-2-l2a", "clouds": True},
    "savi": {"src": "sentinel-2-l2a", "clouds": True},
    "sar": {"src": "sentinel-1-grd", "clouds": False},
    "alerta_fusion": {"fusion": True, "clouds": True},
    "lst": {"src": "sentinel-3-slstr", "clouds": False},
}

EVALSCRIPTS = {
    "rgb": """//VERSION=3
function setup(){return {input:[{bands:["B04","B03","B02","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];return [Math.min(255,Math.round(s.B04*255*3.2)),Math.min(255,Math.round(s.B03*255*3.2)),Math.min(255,Math.round(s.B02*255*3.2)),255];}""",
    "ndvi": """//VERSION=3
function setup(){return {input:[{bands:["B04","B08","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];var v=(s.B08-s.B04)/(s.B08+s.B04+1e-6);if(v<-0.1)return [120,80,40,255];if(v<0.2)return [220,180,30,255];if(v<0.5)return [120,190,40,255];return [15,110,35,255];}""",
    "ndmi": """//VERSION=3
function setup(){return {input:[{bands:["B08","B11","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];var v=(s.B08-s.B11)/(s.B08+s.B11+1e-6);if(v<-0.2)return [177,70,20,255];if(v<0)return [233,142,52,255];if(v<0.2)return [67,160,214,255];return [24,74,194,255];}""",
    "ndwi": """//VERSION=3
function setup(){return {input:[{bands:["B03","B08","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];var v=(s.B03-s.B08)/(s.B03+s.B08+1e-6);if(v<0)return [154,132,98,255];if(v<0.2)return [85,159,200,255];return [0,84,204,255];}""",
    "savi": """//VERSION=3
function setup(){return {input:[{bands:["B04","B08","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];var v=1.5*(s.B08-s.B04)/(s.B08+s.B04+0.5);if(v<0.1)return [210,170,120,255];if(v<0.35)return [150,190,90,255];return [30,120,20,255];}""",
    "sar": """//VERSION=3
function setup(){return {input:[{bands:["VV","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];var vv=Math.max(-25,Math.min(-5,s.VV));if(vv<-17)return [214,84,28,255];if(vv<-12)return [90,160,210,255];return [10,190,210,255];}""",
    "alerta_fusion": """//VERSION=3
function setup(){return {input:[{datasource:"s1",bands:["VV"]},{datasource:"s2",bands:["B08","B11","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(samples){var s1=samples.s1&&samples.s1.length?samples.s1[0]:null;var s2=samples.s2&&samples.s2.length?samples.s2[0]:null;if(!s2||!s2.dataMask)return [0,0,0,0];var ndmi=(s2.B08-s2.B11)/(s2.B08+s2.B11+1e-6);var hum=50;if(s1){hum=Math.min(100,Math.max(0,(s1.VV+18)*10));}var l1=hum<15?3:hum<25?2:hum<50?1:0;var l2=ndmi<-0.10?3:ndmi<0?2:ndmi<0.10?1:0;var lvl=Math.max(l1,l2);if(lvl==3)return [231,76,60,210];if(lvl==2)return [230,126,34,200];if(lvl==1)return [241,196,15,185];return [46,204,113,175];}""",
    "lst": """//VERSION=3
function setup(){return {input:[{bands:["S8","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function evaluatePixel(s){if(!s.dataMask)return [0,0,0,0];var t=s.S8*0.00341802+149.0-273.15;if(t<18)return [0,0,255,255];if(t<28)return [0,255,0,255];if(t<36)return [255,255,0,255];return [255,0,0,255];}""",
}


def tile_to_bbox(z: int, x: int, y: int) -> list[float]:
    n = 2**z
    return [
        round(x / n * 360.0 - 180.0, 6),
        round(math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))), 6),
        round((x + 1) / n * 360.0 - 180.0, 6),
        round(math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n)))), 6),
    ]


def _tile_bucket_key(layer: str, z: int, x: int, y: int, *, target_date: date | None = None) -> str:
    target_date = target_date or date.today()
    return f"tiles/{target_date.isoformat()}/{layer}/{z}/{x}/{y}.png"


def _coneat_bucket_object_key(cache_key: str) -> str:
    return f"external-map-cache/coneat/{cache_key}.bin"


async def fetch_tile_png(layer: str, z: int, x: int, y: int) -> bytes:
    if layer not in EVALSCRIPTS or z < 7 or z > 13:
        return TRANSPARENT_PNG

    today = date.today()
    cache_path = TILE_CACHE_DIR / f"{layer}_{today}_{z}_{x}_{y}.png"
    if cache_path.exists():
        return cache_path.read_bytes()

    tile_bucket_key = _tile_bucket_key(layer, z, x, y, target_date=today)
    bucket_cached = await storage_get_bytes(tile_bucket_key)
    if bucket_cached:
        cache_path.write_bytes(bucket_cached[0])
        return bucket_cached[0]

    if legacy_get_token is None or not settings.copernicus_enabled:
        return TRANSPARENT_PNG

    info = CAPAS_INFO[layer]
    bbox = tile_to_bbox(z, x, y)
    start = str(today - timedelta(days=45))
    data_filter = {"timeRange": {"from": f"{start}T00:00:00Z", "to": f"{today}T23:59:59Z"}}
    if info.get("clouds"):
        data_filter["maxCloudCoverage"] = 50

    if info.get("fusion"):
        data_sources = [
            {"id": "s1", "type": "sentinel-1-grd", "dataFilter": {"timeRange": data_filter["timeRange"]}},
            {"id": "s2", "type": "sentinel-2-l2a", "dataFilter": data_filter},
        ]
    else:
        data_sources = [{"type": info["src"], "dataFilter": data_filter}]

    payload = {
        "input": {
            "bounds": {"bbox": bbox, "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}},
            "data": data_sources,
        },
        "output": {
            "width": 256,
            "height": 256,
            "responses": [{"identifier": "default", "format": {"type": "image/png"}}],
        },
        "evalscript": EVALSCRIPTS[layer],
    }

    try:
        token = await asyncio.to_thread(legacy_get_token)
        response = await asyncio.to_thread(
            lambda: requests.post(
                SH_PROCESS_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "image/png",
                },
                timeout=30,
            )
        )
        if response.status_code == 200 and "image" in response.headers.get("content-type", ""):
            cache_path.write_bytes(response.content)
            await storage_put_bytes(tile_bucket_key, response.content, content_type="image/png")
            return response.content
    except Exception:
        return TRANSPARENT_PNG

    return TRANSPARENT_PNG


async def fetch_rivera_geojson() -> dict:
    if GADM_RIVERA_CACHE.exists():
        return json.loads(GADM_RIVERA_CACHE.read_text(encoding="utf-8"))

    try:
        response = await asyncio.to_thread(lambda: requests.get(GADM_URL, timeout=60))
        response.raise_for_status()
        data = response.json()
        rivera = next(
            (
                feature
                for feature in data.get("features", [])
                if feature.get("properties", {}).get("NAME_1", "").lower() == "rivera"
            ),
            None,
        )
        if rivera:
            geojson = {"type": "FeatureCollection", "features": [rivera]}
            GADM_RIVERA_CACHE.write_text(json.dumps(geojson), encoding="utf-8")
            return geojson
    except Exception:
        pass

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"NAME_1": "Rivera", "source": "fallback"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-57.642, -30.145], [-56.824, -30.000], [-55.971, -30.110],
                        [-55.198, -30.257], [-54.610, -30.383], [-54.178, -30.695],
                        [-53.777, -31.076], [-54.018, -31.514], [-54.572, -31.828],
                        [-55.103, -31.976], [-55.739, -31.857], [-56.368, -31.720],
                        [-56.969, -31.486], [-57.430, -31.165], [-57.642, -30.145],
                    ]],
                },
            }
        ],
    }


def _coneat_request_name(params: dict) -> str:
    return str(params.get("REQUEST") or params.get("request") or "").upper()


def _coneat_default_content_type(params: dict) -> str:
    if _coneat_request_name(params) == "GETMAP":
        return str(params.get("FORMAT") or params.get("format") or "image/png")
    return "text/xml; charset=utf-8"


def _normalize_coneat_params(params: dict) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in params.items():
        key_str = str(key).upper()
        value_str = str(value).strip()
        if key_str == "BBOX":
            try:
                parts = [f"{float(part):.6f}" for part in value_str.split(",")[:4]]
                value_str = ",".join(parts)
            except Exception:
                value_str = value_str
        elif key_str in {"WIDTH", "HEIGHT"}:
            try:
                value_str = str(int(float(value_str)))
            except Exception:
                value_str = value_str
        normalized[key_str] = value_str
    return normalized


def _coneat_cache_key(params: dict) -> str:
    normalized = _normalize_coneat_params(params)
    encoded = urlencode(sorted(normalized.items()), doseq=True)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _coneat_cache_entry_paths(params: dict) -> tuple[Path, Path]:
    digest = _coneat_cache_key(params)
    return CONEAT_CACHE_DIR / f"{digest}.bin", CONEAT_CACHE_DIR / f"{digest}.json"


def _read_coneat_cache(cache_path: Path, meta_path: Path, default_content_type: str) -> tuple[bytes, str] | None:
    if not cache_path.exists():
        return None
    content_type = default_content_type
    if meta_path.exists():
        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            content_type = metadata.get("content_type") or default_content_type
        except Exception:
            content_type = default_content_type
    return cache_path.read_bytes(), content_type


def _write_coneat_cache(cache_path: Path, meta_path: Path, content: bytes, content_type: str) -> None:
    cache_path.write_bytes(content)
    meta_path.write_text(json.dumps({"content_type": content_type}), encoding="utf-8")


async def _read_coneat_cache_db(cache_key: str, default_content_type: str) -> tuple[bytes, str] | None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ExternalMapCacheEntry).where(
                ExternalMapCacheEntry.provider == "coneat",
                ExternalMapCacheEntry.cache_key == cache_key,
            ).limit(1)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        if row.expires_at:
            now = datetime.now(row.expires_at.tzinfo) if row.expires_at.tzinfo else datetime.utcnow()
            if row.expires_at < now:
                await session.delete(row)
                await session.commit()
                return None
        return bytes(row.content), row.content_type or default_content_type


async def _write_coneat_cache_db(
    cache_key: str,
    params: dict,
    content: bytes,
    content_type: str,
) -> None:
    normalized = _normalize_coneat_params(params)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ExternalMapCacheEntry).where(
                ExternalMapCacheEntry.provider == "coneat",
                ExternalMapCacheEntry.cache_key == cache_key,
            ).limit(1)
        )
        row = result.scalar_one_or_none()
        if row is None:
            row = ExternalMapCacheEntry(
                cache_key=cache_key,
                provider="coneat",
                request_name=_coneat_request_name(normalized),
                content_type=content_type,
                content=content,
                content_hash=hashlib.sha1(content).hexdigest(),
                expires_at=datetime.utcnow() + timedelta(hours=settings.coneat_cache_ttl_hours),
                metadata_extra={"params": normalized},
            )
            session.add(row)
        else:
            row.request_name = _coneat_request_name(normalized)
            row.content_type = content_type
            row.content = content
            row.content_hash = hashlib.sha1(content).hexdigest()
            row.expires_at = datetime.utcnow() + timedelta(hours=settings.coneat_cache_ttl_hours)
            row.metadata_extra = {"params": normalized}
        await session.commit()


async def _fetch_coneat_remote(url: str, params: dict) -> tuple[bytes, str]:
    headers = {
        "User-Agent": "AgroClimaX/1.0 CONEAT proxy",
        "Accept": _coneat_default_content_type(params),
    }
    timeout = httpx.Timeout(connect=8.0, read=30.0, write=15.0, pool=15.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
    return response.content, response.headers.get("content-type", _coneat_default_content_type(params))


async def proxy_coneat_request(params: dict) -> tuple[bytes, str]:
    url = "http://dgrn.mgap.gub.uy/arcgis/services/TEMATICOS/IntConeat/MapServer/WMSServer"
    normalized_params = _normalize_coneat_params(params)
    default_content_type = _coneat_default_content_type(normalized_params)
    cache_key = _coneat_cache_key(normalized_params)
    cache_path, meta_path = _coneat_cache_entry_paths(normalized_params)
    cached = _read_coneat_cache(cache_path, meta_path, default_content_type)
    if cached:
        return cached
    cached = await _read_coneat_cache_db(cache_key, default_content_type)
    if cached:
        _write_coneat_cache(cache_path, meta_path, cached[0], cached[1])
        return cached
    bucket_cached = await storage_get_bytes(_coneat_bucket_object_key(cache_key))
    if bucket_cached:
        content, content_type, _metadata = bucket_cached
        resolved_content_type = content_type or default_content_type
        _write_coneat_cache(cache_path, meta_path, content, resolved_content_type)
        await _write_coneat_cache_db(cache_key, normalized_params, content, resolved_content_type)
        return content, resolved_content_type

    last_error: Exception | None = None
    async with CONEAT_PROXY_SEMAPHORE:
        cached = _read_coneat_cache(cache_path, meta_path, default_content_type)
        if cached:
            return cached
        cached = await _read_coneat_cache_db(cache_key, default_content_type)
        if cached:
            _write_coneat_cache(cache_path, meta_path, cached[0], cached[1])
            return cached
        bucket_cached = await storage_get_bytes(_coneat_bucket_object_key(cache_key))
        if bucket_cached:
            content, content_type, _metadata = bucket_cached
            resolved_content_type = content_type or default_content_type
            _write_coneat_cache(cache_path, meta_path, content, resolved_content_type)
            await _write_coneat_cache_db(cache_key, normalized_params, content, resolved_content_type)
            return content, resolved_content_type

        for attempt, delay in enumerate(CONEAT_PROXY_RETRY_DELAYS, start=1):
            try:
                content, content_type = await _fetch_coneat_remote(url, normalized_params)
                _write_coneat_cache(cache_path, meta_path, content, content_type)
                await _write_coneat_cache_db(cache_key, normalized_params, content, content_type)
                await storage_put_bytes(
                    _coneat_bucket_object_key(cache_key),
                    content,
                    content_type=content_type,
                )
                return content, content_type
            except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt < len(CONEAT_PROXY_RETRY_DELAYS):
                    await asyncio.sleep(delay)

    cached = _read_coneat_cache(cache_path, meta_path, default_content_type)
    if cached:
        return cached
    cached = await _read_coneat_cache_db(cache_key, default_content_type)
    if cached:
        _write_coneat_cache(cache_path, meta_path, cached[0], cached[1])
        return cached
    bucket_cached = await storage_get_bytes(_coneat_bucket_object_key(cache_key))
    if bucket_cached:
        content, content_type, _metadata = bucket_cached
        resolved_content_type = content_type or default_content_type
        _write_coneat_cache(cache_path, meta_path, content, resolved_content_type)
        await _write_coneat_cache_db(cache_key, normalized_params, content, resolved_content_type)
        return content, resolved_content_type

    if _coneat_request_name(normalized_params) == "GETMAP":
        return TRANSPARENT_PNG, "image/png"

    if last_error is not None:
        message = str(last_error).replace("&", "and").replace("<", "").replace(">", "")
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<ServiceExceptionReport>"
            f"<ServiceException>{message}</ServiceException>"
            "</ServiceExceptionReport>"
        ).encode("utf-8")
        return xml, "text/xml; charset=utf-8"

    return b"", default_content_type


def _iter_geometry_coordinates(node):
    if isinstance(node, (list, tuple)):
        if len(node) >= 2 and isinstance(node[0], (int, float)) and isinstance(node[1], (int, float)):
            yield float(node[0]), float(node[1])
            return
        for item in node:
            yield from _iter_geometry_coordinates(item)


def _geometry_bounds(geometry: dict | None) -> tuple[float, float, float, float] | None:
    if not geometry:
        return None
    coords = list(_iter_geometry_coordinates(geometry.get("coordinates")))
    if not coords:
        return None
    lons = [lon for lon, _ in coords]
    lats = [lat for _, lat in coords]
    return min(lons), min(lats), max(lons), max(lats)


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


def _tile_ranges_for_bounds(bounds: tuple[float, float, float, float], zoom: int) -> tuple[range, range]:
    west, south, east, north = bounds
    x_min = _lon_to_tile_x(west, zoom)
    x_max = _lon_to_tile_x(east, zoom)
    y_min = _lat_to_tile_y(north, zoom)
    y_max = _lat_to_tile_y(south, zoom)
    return range(min(x_min, x_max), max(x_min, x_max) + 1), range(min(y_min, y_max), max(y_min, y_max) + 1)


def _build_coneat_map_params(z: int, x: int, y: int) -> dict[str, str]:
    bbox = tile_to_bbox(z, x, y)
    return _normalize_coneat_params(
        {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "LAYERS": "2,5",
            "STYLES": "",
            "FORMAT": "image/png",
            "TRANSPARENT": "true",
            "VERSION": "1.1.1",
            "WIDTH": "256",
            "HEIGHT": "256",
            "SRS": "EPSG:4326",
            "BBOX": ",".join(f"{value:.6f}" for value in bbox),
        }
    )


async def prewarm_coneat_tiles(department: str | None = None) -> dict[str, object]:
    zoom_levels = sorted({int(level) for level in settings.coneat_prewarm_zoom_levels})
    national_bounds = (-58.7, -35.2, -53.0, -30.0)
    viewport_specs: list[tuple[str, tuple[float, float, float, float], list[int]]] = [
        ("nacional", national_bounds, [level for level in zoom_levels if level <= 7] or zoom_levels),
    ]

    async with AsyncSessionLocal() as session:
        query = select(AOIUnit).where(AOIUnit.active.is_(True), AOIUnit.unit_type == "department")
        if department:
            query = query.where(AOIUnit.department == department)
        result = await session.execute(query)
        units = result.scalars().all()

    for unit in units:
        bounds = _geometry_bounds(unit.geometry_geojson)
        if bounds is None:
            continue
        viewport_specs.append((unit.department, bounds, [level for level in zoom_levels if level >= 7] or zoom_levels))

    tile_params: dict[str, dict[str, str]] = {}
    for _, bounds, spec_zooms in viewport_specs:
        for zoom in spec_zooms:
            x_range, y_range = _tile_ranges_for_bounds(bounds, zoom)
            for x in x_range:
                for y in y_range:
                    params = _build_coneat_map_params(zoom, x, y)
                    tile_params[_coneat_cache_key(params)] = params

    warmed = 0
    reused = 0
    for params in tile_params.values():
        cache_key = _coneat_cache_key(params)
        cache_path, meta_path = _coneat_cache_entry_paths(params)
        if _read_coneat_cache(cache_path, meta_path, "image/png") or await _read_coneat_cache_db(cache_key, "image/png"):
            reused += 1
            continue
        await proxy_coneat_request(params)
        warmed += 1

    return {
        "status": "success",
        "department_filter": department,
        "zoom_levels": zoom_levels,
        "planned_tiles": len(tile_params),
        "reused_tiles": reused,
        "warmed_tiles": warmed,
        "cache_backend": "database+filesystem+s3" if settings.storage_bucket_enabled else "database+filesystem",
    }
