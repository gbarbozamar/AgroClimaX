from __future__ import annotations

import asyncio
import hashlib
import io
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import time
from typing import Any
from urllib.parse import urlencode

import httpx
import requests
from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.humedad import AOIUnit
from app.models.materialized import ExternalMapCacheEntry, RasterCacheEntry, SatelliteLayerSnapshot
from app.services.object_storage import storage_get_bytes, storage_put_bytes
from app.services.raster_products import (
    get_canonical_product_frame_metadata,
    get_canonical_product_status_index,
    read_scope_viewport_raster_fallback_tile,
    read_viewport_raster_product_tile,
    render_canonical_raster_tile,
)
from app.services.tileserver_client import fetch_tileserver_tile
from app.services.raster_cache import (
    get_raster_cache_status_index,
    parse_bbox_values,
    raster_cache_key,
    upsert_raster_cache_entry,
    viewport_bucket,
)

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
CONEAT_CACHE_NAMESPACE = "renare_export_v1"
CONEAT_EXPORT_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/MapasBase/Renare_Coneat/MapServer/export"
CONEAT_INFO_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/MapasBase/Renare_Coneat/MapServer"
OFFICIAL_OVERLAY_CACHE_DIR = Path(__file__).resolve().parents[2] / ".official_overlay_cache"
OFFICIAL_OVERLAY_CACHE_DIR.mkdir(exist_ok=True)
OFFICIAL_OVERLAY_PROXY_SEMAPHORE = asyncio.Semaphore(4)
OFFICIAL_OVERLAY_PROXY_RETRY_DELAYS = (0.35, 1.0, 2.0)
TIMELINE_MANIFEST_CACHE_TTL_SECONDS = 900
TIMELINE_FRAME_WINDOW_DAYS = settings.timeline_historical_window_days
TIMELINE_MANIFEST_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
TIMELINE_SOURCE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
VISUAL_EMPTY_RENDERABLE_THRESHOLD_PCT = 5.0

SNIG_IMAGE_EXPORT_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/MapasBase/SNIG_Image/MapServer/export"
SNIG_CATASTRO_EXPORT_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/Uruguay/SNIG_Catastro/MapServer/export"
DGSA_ZONAS_SENSIBLES_EXPORT_URL = "https://web.snig.gub.uy/arcgisserver/rest/services/DGSA/ZonasSensibles/MapServer/export"

OFFICIAL_MAP_OVERLAYS: dict[str, dict[str, object]] = {
    "coneat": {
        "id": "coneat",
        "label": "CONEAT",
        "category": "Suelos",
        "provider": "SNIG / MGAP",
        "service_kind": "arcgis_export",
        "service_url": CONEAT_EXPORT_URL,
        "layers": "show:0,1",
        "min_zoom": 11,
        "opacity_default": 0.96,
        "z_index_priority": 330,
        "attribution": "SNIG / MGAP Uruguay",
        "cache_namespace": "renare_export_v1",
        "recommended": True,
    },
    "hidrografia": {
        "id": "hidrografia",
        "label": "Hidrografia",
        "category": "Agua",
        "provider": "SNIG",
        "service_kind": "arcgis_export",
        "service_url": SNIG_IMAGE_EXPORT_URL,
        "layers": "show:14,15,16,17,18,19",
        "min_zoom": 9,
        "opacity_default": 0.84,
        "z_index_priority": 340,
        "attribution": "SNIG Uruguay",
        "cache_namespace": "snig_hidrografia_v1",
        "recommended": True,
    },
    "area_inundable": {
        "id": "area_inundable",
        "label": "Area inundable",
        "category": "Agua",
        "provider": "SNIG",
        "service_kind": "arcgis_export",
        "service_url": SNIG_IMAGE_EXPORT_URL,
        "layers": "show:13",
        "min_zoom": 10,
        "opacity_default": 0.76,
        "z_index_priority": 341,
        "attribution": "SNIG Uruguay",
        "cache_namespace": "snig_area_inundable_v1",
        "recommended": True,
    },
    "catastro_rural": {
        "id": "catastro_rural",
        "label": "Catastro rural",
        "category": "Parcelas",
        "provider": "SNIG / Catastro",
        "service_kind": "arcgis_export",
        "service_url": SNIG_CATASTRO_EXPORT_URL,
        "layers": "show:0",
        "min_zoom": 12,
        "opacity_default": 0.92,
        "z_index_priority": 350,
        "attribution": "SNIG / Catastro Uruguay",
        "cache_namespace": "snig_catastro_rural_v1",
        "recommended": True,
    },
    "rutas_camineria": {
        "id": "rutas_camineria",
        "label": "Rutas y camineria",
        "category": "Infraestructura",
        "provider": "SNIG",
        "service_kind": "arcgis_export",
        "service_url": SNIG_IMAGE_EXPORT_URL,
        "layers": "show:8,9,10,11",
        "min_zoom": 9,
        "opacity_default": 0.8,
        "z_index_priority": 320,
        "attribution": "SNIG Uruguay",
        "cache_namespace": "snig_rutas_camineria_v1",
        "recommended": True,
    },
    "zonas_sensibles": {
        "id": "zonas_sensibles",
        "label": "Zonas sensibles",
        "category": "Restricciones",
        "provider": "DGSA",
        "service_kind": "arcgis_export",
        "service_url": DGSA_ZONAS_SENSIBLES_EXPORT_URL,
        "layers": "show:0,3,7",
        "min_zoom": 11,
        "opacity_default": 0.74,
        "z_index_priority": 345,
        "attribution": "DGSA / MGAP Uruguay",
        "cache_namespace": "dgsa_zonas_sensibles_v1",
        "recommended": True,
    },
}

TRANSPARENT_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0bIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00"
    b"\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
)
VISUAL_EMPTY_TILE_ALPHA_THRESHOLD_PCT = 0.5
LAYER_VISIBLE_TILE_THRESHOLD_PCT = {
    "alerta_fusion": 5.0,
    "rgb": 2.0,
    "ndvi": 2.0,
    "ndmi": 2.0,
    "ndwi": 2.0,
    "savi": 2.0,
}
LAYER_PREFERRED_TILE_TARGET_PCT = {
    "alerta_fusion": 85.0,
    "rgb": 85.0,
    "ndvi": 80.0,
    "ndmi": 80.0,
    "ndwi": 80.0,
    "savi": 80.0,
}
LAYER_PREFERRED_TILE_TEXTURE_SCORE = {
    "alerta_fusion": 120.0,
}
LAYER_MAX_CLOUDLIKE_TILE_PCT = {
    "rgb": 35.0,
}
LAYER_MAX_CLOUDLIKE_VISIBLE_PCT = {
    "rgb": 12.0,
}
RUNTIME_PROBE_MIN_SAMPLE_COVERAGE_RATIO = {
    "alerta_fusion": 0.78,
    "rgb": 0.72,
    "ndvi": 0.68,
    "ndmi": 0.68,
    "ndwi": 0.68,
    "savi": 0.68,
}
RUNTIME_PROBE_MIN_AVG_VISIBLE_PCT = {
    "alerta_fusion": 14.0,
    "rgb": 8.0,
    "ndvi": 7.0,
    "ndmi": 7.0,
    "ndwi": 7.0,
    "savi": 7.0,
}
RUNTIME_PROBE_MIN_GOOD_TILE_RATIO = {
    "alerta_fusion": 0.72,
    "rgb": 0.82,
    "ndvi": 0.74,
    "ndmi": 0.74,
    "ndwi": 0.74,
    "savi": 0.74,
}
RUNTIME_BUCKET_PROBE_VERSION = 2
RUNTIME_PROBE_MAX_ATTEMPTS = 6

TILE_MIN_ZOOM = 7
TILE_MAX_ZOOM = 17
TEMPORAL_LAYER_ALIASES = {
    "alerta": "alerta_fusion",
    "rgb": "rgb",
    "ndvi": "ndvi",
    "ndmi": "ndmi",
    "ndwi": "ndwi",
    "savi": "savi",
    "sar": "sar",
    "lst": "lst",
}
TEMPORAL_LAYER_PUBLIC_IDS = {internal: public for public, internal in TEMPORAL_LAYER_ALIASES.items()}
TEMPORAL_LAYER_CONFIGS: dict[str, dict[str, Any]] = {
    "alerta_fusion": {
        "public_id": "alerta",
        "label": "Alerta",
        "revisit_days": 1,
        "window_before_days": 14,
        "window_after_days": 0,
        "time_mode": "carry_forward",
        "anchor_date": date(2020, 1, 1),
    },
    "rgb": {
        "public_id": "rgb",
        "label": "RGB",
        "revisit_days": 5,
        "window_before_days": 2,
        "window_after_days": 2,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 3),
    },
    "ndvi": {
        "public_id": "ndvi",
        "label": "NDVI",
        "revisit_days": 5,
        "window_before_days": 2,
        "window_after_days": 2,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 3),
    },
    "ndmi": {
        "public_id": "ndmi",
        "label": "NDMI",
        "revisit_days": 5,
        "window_before_days": 2,
        "window_after_days": 2,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 3),
    },
    "ndwi": {
        "public_id": "ndwi",
        "label": "NDWI",
        "revisit_days": 5,
        "window_before_days": 2,
        "window_after_days": 2,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 3),
    },
    "savi": {
        "public_id": "savi",
        "label": "SAVI",
        "revisit_days": 5,
        "window_before_days": 2,
        "window_after_days": 2,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 3),
    },
    "sar": {
        "public_id": "sar",
        "label": "SAR VV",
        "revisit_days": 6,
        "window_before_days": 3,
        "window_after_days": 3,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 2),
    },
    "lst": {
        "public_id": "lst",
        "label": "Termal",
        "revisit_days": 1,
        "window_before_days": 1,
        "window_after_days": 1,
        "time_mode": "symmetric",
        "anchor_date": date(2020, 1, 1),
    },
}

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
function setup(){return {input:[{datasource:"s1",bands:["VV","dataMask"]},{datasource:"s2",bands:["B08","B11","dataMask"]}],output:{bands:4,sampleType:"UINT8"}};}
function severityColor(level, alpha){if(level>=3)return [231,76,60,alpha];if(level>=2)return [230,126,34,alpha];if(level>=1)return [241,196,15,alpha];return [46,204,113,Math.max(140,alpha-35)];}
function pickValidSample(sampleOrSamples, validator){
  if(!sampleOrSamples)return null;
  if(Array.isArray(sampleOrSamples)){
    for(var i=0;i<sampleOrSamples.length;i++){
      var candidate=sampleOrSamples[i];
      if(validator(candidate))return candidate;
    }
    return sampleOrSamples.length?sampleOrSamples[0]:null;
  }
  return validator(sampleOrSamples)?sampleOrSamples:sampleOrSamples;
}
function evaluatePixel(samples){
var s1=pickValidSample(samples.s1,function(item){return !!(item&&((item.dataMask===undefined)||item.dataMask)&&isFinite(item.VV));});
var s2=pickValidSample(samples.s2,function(item){return !!(item&&item.dataMask&&isFinite(item.B08)&&isFinite(item.B11));});
var s1Valid=!!(s1&&((s1.dataMask===undefined)||s1.dataMask)&&isFinite(s1.VV));
var s2Valid=!!(s2&&s2.dataMask&&isFinite(s2.B08)&&isFinite(s2.B11));
if(!s1Valid&&!s2Valid)return [0,0,0,0];
var hum=null;
if(s1Valid){hum=Math.min(100,Math.max(0,(s1.VV+18)*10));}
var l1=hum===null?-1:(hum<15?3:hum<25?2:hum<50?1:0);
if(s2Valid){
  var ndmi=(s2.B08-s2.B11)/(s2.B08+s2.B11+1e-6);
  var l2=ndmi<-0.10?3:ndmi<0?2:ndmi<0.10?1:0;
  var lvl=l1<0?l2:Math.max(l1,l2);
  return severityColor(lvl,s1Valid?210:195);
}
return [0,0,0,0];
}""",
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


def _normalized_scope_ref(
    *,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
) -> str:
    normalized_scope_type = str(scope_type or scope or "").strip().lower()
    if unit_id and normalized_scope_type not in {"", "nacional", "global", "departamento"}:
        return str(unit_id)
    if scope_ref:
        return str(scope_ref)
    if unit_id:
        return str(unit_id)
    if department:
        return str(department)
    if scope_type:
        return str(scope_type)
    if scope:
        return str(scope)
    return "global"


def _coneat_bucket_object_key(cache_key: str) -> str:
    return f"external-map-cache/coneat/{cache_key}.bin"


def resolve_temporal_layer_id(layer: str) -> str | None:
    candidate = str(layer or "").strip().lower()
    if candidate in TEMPORAL_LAYER_ALIASES:
        return TEMPORAL_LAYER_ALIASES[candidate]
    if candidate in TEMPORAL_LAYER_CONFIGS:
        return candidate
    return None


def _effective_source_date(target_date: date | None) -> date:
    if target_date is None:
        return date.today()
    return min(target_date, date.today())


def _parse_bbox_string(bbox: str | None) -> tuple[float, float, float, float] | None:
    return parse_bbox_values(bbox)


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


def _tile_coords_for_bbox(bbox: str | None, zoom: int) -> list[tuple[int, int]]:
    resolved = _parse_bbox_string(bbox)
    if resolved is None:
        return []
    west, south, east, north = resolved
    x_min = _lon_to_tile_x(west, zoom)
    x_max = _lon_to_tile_x(east, zoom)
    y_min = _lat_to_tile_y(north, zoom)
    y_max = _lat_to_tile_y(south, zoom)
    coords = [
        (x, y)
        for x in range(min(x_min, x_max), max(x_min, x_max) + 1)
        for y in range(min(y_min, y_max), max(y_min, y_max) + 1)
    ]
    return coords


def _sample_tile_coords_for_bbox(bbox: str | None, zoom: int, *, limit: int = 9) -> list[tuple[int, int]]:
    coords = _tile_coords_for_bbox(bbox, zoom)
    if len(coords) <= limit:
        return coords
    xs = sorted({x for x, _ in coords})
    ys = sorted({y for _, y in coords})

    def _pick(values: list[int]) -> list[int]:
        if len(values) <= 3:
            return values
        return [values[0], values[len(values) // 2], values[-1]]

    sampled = {(x, y) for x in _pick(xs) for y in _pick(ys)}
    ordered = [coord for coord in coords if coord in sampled]
    return ordered[:limit]


def _normalized_temporal_zoom(zoom: int | None, *, tile_zoom: int | None = None) -> int:
    candidates: list[int] = []
    for value in (zoom, tile_zoom):
        try:
            if value is None:
                continue
            candidates.append(int(value))
        except Exception:
            continue
    if not candidates:
        return TILE_MIN_ZOOM
    resolved = min(candidates)
    return max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, resolved))


def _canonical_internal_zoom_for_layer(layer: str) -> int:
    normalized = str(layer or "").strip().lower()
    optical_zoom = int(getattr(settings, "raster_canonical_zoom_optical", 14))
    sar_zoom = int(getattr(settings, "raster_canonical_zoom_sar", 14))
    lst_zoom = int(getattr(settings, "raster_canonical_zoom_lst", 11))
    alerta_zoom = int(getattr(settings, "raster_canonical_zoom_alerta", optical_zoom))
    if normalized == "lst":
        return max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, lst_zoom))
    if normalized == "sar":
        return max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, sar_zoom))
    if normalized == "alerta_fusion":
        return max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, alerta_zoom))
    return max(TILE_MIN_ZOOM, min(TILE_MAX_ZOOM, optical_zoom))


def _source_metadata_is_known_empty(metadata: dict[str, Any]) -> bool:
    visual_state = str(metadata.get("visual_state") or "").strip().lower()
    if visual_state == "empty":
        return True
    if visual_state == "missing":
        return False
    return bool(metadata.get("visual_empty"))


def _public_layer_id(layer: str) -> str:
    return str(TEMPORAL_LAYER_CONFIGS.get(layer, {}).get("public_id") or layer)


def _normalized_timeline_scope_type(scope_type: str | None, scope: str | None = None) -> str:
    if scope_type:
        return str(scope_type)
    if scope:
        return str(scope)
    return "global"


def _serve_tiles_internal_enabled() -> bool:
    return bool(getattr(settings, "serve_tiles_internal", True))


def _disable_heuristic_ready_enabled() -> bool:
    return bool(getattr(settings, "disable_heuristic_ready", False))


def _normalized_internal_only_layers() -> set[str]:
    configured = getattr(settings, "internal_only_layers", []) or []
    normalized: set[str] = set()
    for item in configured:
        candidate = str(item or "").strip().lower()
        if not candidate:
            continue
        resolved = resolve_temporal_layer_id(candidate)
        normalized.add(resolved or candidate)
    return normalized


def _normalized_internal_only_scopes() -> set[str]:
    configured = getattr(settings, "internal_only_scopes", []) or []
    return {str(item or "").strip().lower() for item in configured if str(item or "").strip()}


def _internal_only_cutover_active(layer: str, scope_type: str | None) -> bool:
    layer_filters = _normalized_internal_only_layers()
    scope_filters = _normalized_internal_only_scopes()
    if not layer_filters and not scope_filters:
        return False
    normalized_scope = str(scope_type or "nacional").strip().lower() or "nacional"
    layer_match = not layer_filters or str(layer or "").strip().lower() in layer_filters
    scope_match = not scope_filters or normalized_scope in scope_filters
    return layer_match and scope_match


def _timeline_source_status(metadata: dict[str, Any]) -> str:
    visual_state = str(metadata.get("visual_state") or "")
    if visual_state in {"empty", "missing"}:
        return visual_state
    return "ready"


def _is_persistable_timeline_source_metadata(metadata: dict[str, Any]) -> bool:
    coverage_origin = str(metadata.get("coverage_origin") or "")
    selection_reason = str(metadata.get("selection_reason") or "")
    if coverage_origin == "runtime_tile_unlock_fallback":
        return False
    if selection_reason in {"runtime_bucket_probe", "runtime_bucket_carry_forward"}:
        if int(metadata.get("probe_version") or 0) < RUNTIME_BUCKET_PROBE_VERSION:
            return False
        if metadata.get("good_tile_ratio") is None:
            return False
    return True


def _is_runtime_resolved_source(metadata: dict[str, Any]) -> bool:
    return str(metadata.get("selection_reason") or "") in {"runtime_bucket_probe", "runtime_bucket_carry_forward"} or bool(
        metadata.get("source_locked")
    )


def _png_visible_pixel_pct(content: bytes) -> float | None:
    if not content or content == TRANSPARENT_PNG:
        return 0.0
    try:
        from PIL import Image

        with Image.open(io.BytesIO(content)) as image:
            rgba = image.convert("RGBA")
            alpha = rgba.getchannel("A")
            histogram = alpha.histogram()
            opaque_pixels = sum(histogram[1:])
            total_pixels = max(rgba.size[0] * rgba.size[1], 1)
            return (opaque_pixels / total_pixels) * 100.0
    except Exception:
        return None


def _tile_content_is_visually_empty(content: bytes, *, layer: str | None = None) -> bool:
    visible_pixel_pct = _png_visible_pixel_pct(content)
    if visible_pixel_pct is None:
        return False
    threshold = LAYER_VISIBLE_TILE_THRESHOLD_PCT.get(str(layer or ""), VISUAL_EMPTY_TILE_ALPHA_THRESHOLD_PCT)
    return visible_pixel_pct < threshold


def _png_cloudlike_quality(content: bytes, *, block_size: int = 64) -> tuple[float, float]:
    if not content or content == TRANSPARENT_PNG:
        return 0.0, 0.0
    try:
        from PIL import Image

        with Image.open(io.BytesIO(content)) as image:
            rgba = image.convert("RGBA")
            width, height = rgba.size
            block_totals: dict[tuple[int, int], int] = {}
            block_clouds: dict[tuple[int, int], int] = {}
            opaque_pixels = 0
            cloudlike_pixels = 0
            for y in range(height):
                for x in range(width):
                    r, g, b, a = rgba.getpixel((x, y))
                    if a <= 0:
                        continue
                    opaque_pixels += 1
                    block_key = (x // max(block_size, 1), y // max(block_size, 1))
                    block_totals[block_key] = block_totals.get(block_key, 0) + 1
                    if r > 215 and g > 215 and b > 215 and (max(r, g, b) - min(r, g, b)) < 24:
                        cloudlike_pixels += 1
                        block_clouds[block_key] = block_clouds.get(block_key, 0) + 1
            if opaque_pixels <= 0:
                return 0.0, 0.0
            cloudlike_pct = (float(cloudlike_pixels) / float(opaque_pixels)) * 100.0
            max_block_pct = 0.0
            for block_key, total in block_totals.items():
                if total <= 0:
                    continue
                pct = (float(block_clouds.get(block_key, 0)) / float(total)) * 100.0
                if pct > max_block_pct:
                    max_block_pct = pct
            return round(cloudlike_pct, 2), round(max_block_pct, 2)
    except Exception:
        return 0.0, 0.0


def _tile_content_is_good_enough(content: bytes, *, layer: str | None = None) -> bool:
    visible_pixel_pct = _png_visible_pixel_pct(content)
    if visible_pixel_pct is None:
        return True
    threshold = LAYER_PREFERRED_TILE_TARGET_PCT.get(str(layer or ""))
    if threshold is None:
        return not _tile_content_is_visually_empty(content, layer=layer)
    if visible_pixel_pct < threshold:
        return False
    cloudlike_threshold = LAYER_MAX_CLOUDLIKE_VISIBLE_PCT.get(str(layer or ""))
    cloudlike_tile_threshold = LAYER_MAX_CLOUDLIKE_TILE_PCT.get(str(layer or ""))
    if cloudlike_threshold is not None or cloudlike_tile_threshold is not None:
        cloudlike_pct, cloudlike_max_tile_pct = _png_cloudlike_quality(content)
        if cloudlike_threshold is not None and cloudlike_pct >= cloudlike_threshold:
            return False
        if cloudlike_tile_threshold is not None and cloudlike_max_tile_pct >= cloudlike_tile_threshold:
            return False
    texture_threshold = LAYER_PREFERRED_TILE_TEXTURE_SCORE.get(str(layer or ""))
    if texture_threshold is None:
        return True
    return _png_texture_score(content) >= texture_threshold


def _png_texture_score(content: bytes) -> float:
    if not content or content == TRANSPARENT_PNG:
        return 0.0
    try:
        from PIL import Image

        with Image.open(io.BytesIO(content)) as image:
            rgba = image.convert("RGBA")
            width, height = rgba.size
            step = max(min(width, height) // 32, 8)
            transitions = 0
            for y in range(0, height, step):
                prev_pixel = None
                for x in range(0, width, step):
                    pixel = rgba.getpixel((x, y))
                    if pixel[3] <= 0:
                        continue
                    if prev_pixel is not None:
                        delta = abs(pixel[0] - prev_pixel[0]) + abs(pixel[1] - prev_pixel[1]) + abs(pixel[2] - prev_pixel[2])
                        if delta > 12:
                            transitions += 1
                    prev_pixel = pixel
            return float(transitions)
    except Exception:
        return 0.0


def _tile_quality_metrics(content: bytes, *, layer: str) -> dict[str, float | bool]:
    visible_pct = _png_visible_pixel_pct(content)
    texture_score = _png_texture_score(content)
    cloudlike_pct, cloudlike_max_tile_pct = _png_cloudlike_quality(content)
    visually_empty = _tile_content_is_visually_empty(content, layer=layer)
    return {
        "visible_pct": float(visible_pct or 0.0),
        "texture_score": float(texture_score or 0.0),
        "cloudlike_pct": float(cloudlike_pct or 0.0),
        "cloudlike_max_tile_pct": float(cloudlike_max_tile_pct or 0.0),
        "good_enough": _tile_content_is_good_enough(content, layer=layer),
        "visually_empty": visually_empty,
    }

def _time_range_for_temporal_layer(layer: str, target_date: date) -> tuple[date, date]:
    config = TEMPORAL_LAYER_CONFIGS.get(layer)
    if not config:
        return target_date - timedelta(days=45), target_date
    before_days = int(config.get("window_before_days", 0))
    after_days = int(config.get("window_after_days", 0))
    if config.get("time_mode") == "carry_forward":
        return target_date - timedelta(days=before_days), target_date
    return target_date - timedelta(days=before_days), target_date + timedelta(days=after_days)


def _runtime_probe_attempt_candidates(
    *,
    layer: str,
    display_date: date,
    fallback_metadata: dict[str, Any],
) -> list[tuple[date, bool]]:
    probe_metadata = dict(fallback_metadata or {})
    if str(probe_metadata.get("selection_reason") or "") == "heuristic_fallback":
        # Heuristic placeholders should not lock the runtime probe to the
        # display date; otherwise national optical/fusion layers never search
        # nearby usable acquisitions and the manifest collapses to "missing".
        probe_metadata["source_locked"] = False
        probe_metadata.pop("resolved_source_date", None)
    attempts = _temporal_tile_request_attempts(
        layer=layer,
        effective_date=display_date,
        frame_role="primary",
        source_metadata=probe_metadata,
    )
    deduped: list[tuple[date, bool]] = []
    seen: set[tuple[date, bool]] = set()
    for source_date, widen_window in attempts:
        key = (_effective_source_date(source_date), bool(widen_window))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
        if len(deduped) >= RUNTIME_PROBE_MAX_ATTEMPTS:
            break
    return deduped


def _build_temporal_data_sources(
    layer: str,
    source_date: date,
    *,
    widen_window: bool = False,
) -> list[dict[str, Any]]:
    info = CAPAS_INFO[layer]
    start_date, end_date = _time_range_for_temporal_attempt(layer, source_date, widen_window=widen_window)
    data_filter: dict[str, Any] = {
        "timeRange": {
            "from": f"{start_date.isoformat()}T00:00:00Z",
            "to": f"{end_date.isoformat()}T23:59:59Z",
        }
    }
    if info.get("clouds"):
        data_filter["maxCloudCoverage"] = 80 if widen_window else 50
        data_filter["mosaickingOrder"] = "leastCC"
    if info.get("fusion"):
        s1_start_date = max(source_date - timedelta(days=12), date(2020, 1, 1))
        s1_filter = {
            "timeRange": {
                "from": f"{s1_start_date.isoformat()}T00:00:00Z",
                "to": f"{source_date.isoformat()}T23:59:59Z",
            }
        }
        return [
            {"id": "s1", "type": "sentinel-1-grd", "dataFilter": s1_filter},
            {"id": "s2", "type": "sentinel-2-l2a", "dataFilter": data_filter},
        ]
    return [{"type": info["src"], "dataFilter": data_filter}]


def _build_temporal_request_payload(
    *,
    layer: str,
    bbox: list[float],
    display_date: date,
    source_date: date,
    widen_window: bool,
    frame_role: str | None,
    source_metadata: dict[str, Any],
    scope: str | None,
    unit_id: str | None,
    department: str | None,
    scope_type: str | None,
    scope_ref: str | None,
    bbox_bucket: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "input": {
            "bounds": {"bbox": bbox, "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}},
            "data": _build_temporal_data_sources(layer, source_date, widen_window=widen_window),
        },
        "output": {
            "width": 256,
            "height": 256,
            "responses": [{"identifier": "default", "format": {"type": "image/png"}}],
        },
        "evalscript": EVALSCRIPTS[layer],
    }
    metadata = {
        "display_date": display_date.isoformat(),
        "source_date": source_date.isoformat(),
        "secondary_source_date": source_metadata.get("secondary_source_date"),
        "availability": source_metadata.get("availability"),
        "scope": scope,
        "unit_id": unit_id,
        "department": department,
        "scope_type": scope_type,
        "scope_ref": scope_ref,
        "bbox_bucket": bbox_bucket,
        "visual_state": source_metadata.get("visual_state"),
        "renderable_pixel_pct": source_metadata.get("renderable_pixel_pct"),
        "fusion_mode": source_metadata.get("fusion_mode"),
        "selection_reason": source_metadata.get("selection_reason"),
        "widen_window": widen_window,
    }
    if frame_role:
        metadata["frame_role"] = frame_role
    payload["metadata"] = metadata
    return payload


async def _fetch_temporal_tile_attempt(
    *,
    layer: str,
    z: int,
    x: int,
    y: int,
    display_date: date,
    source_date: date,
    widen_window: bool,
    token: str,
    frame_role: str | None,
    source_metadata: dict[str, Any],
    scope: str | None,
    unit_id: str | None,
    department: str | None,
    scope_type: str | None,
    scope_ref: str | None,
    bbox_bucket: str | None,
    use_internal_products: bool = True,
) -> bytes | None:
    cache_path = TILE_CACHE_DIR / f"{layer}_{source_date.isoformat()}_{z}_{x}_{y}.png"
    if cache_path.exists():
        cached_content = cache_path.read_bytes()
        if not _tile_content_is_visually_empty(cached_content, layer=layer):
            return cached_content
    tile_bucket_key = _tile_bucket_key(layer, z, x, y, target_date=source_date)
    bucket_cached = await storage_get_bytes(tile_bucket_key)
    if bucket_cached:
        bucket_content = bucket_cached[0]
        if not _tile_content_is_visually_empty(bucket_content, layer=layer):
            cache_path.write_bytes(bucket_content)
            return bucket_content

    bbox = tile_to_bbox(z, x, y)
    payload = _build_temporal_request_payload(
        layer=layer,
        bbox=bbox,
        display_date=display_date,
        source_date=source_date,
        widen_window=widen_window,
        frame_role=frame_role,
        source_metadata=source_metadata,
        scope=scope,
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=scope_ref,
        bbox_bucket=bbox_bucket,
    )
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
    if response.status_code != 200 or "image" not in response.headers.get("content-type", ""):
        return None
    if _tile_content_is_visually_empty(response.content, layer=layer):
        return None
    cache_path.write_bytes(response.content)
    await storage_put_bytes(tile_bucket_key, response.content, content_type="image/png")
    return response.content


async def _probe_runtime_bucket_source_metadata(
    *,
    layer: str,
    display_date: date,
    bbox: str,
    zoom: int,
    fallback_metadata: dict[str, Any],
    scope: str | None,
    unit_id: str | None,
    department: str | None,
    scope_type: str | None,
    scope_ref: str | None,
    bbox_bucket: str | None,
) -> dict[str, Any] | None:
    if legacy_get_token is None or not settings.copernicus_enabled:
        return None
    sample_coords = _tile_coords_for_bbox(bbox, zoom)
    if len(sample_coords) > 36:
        sample_coords = _sample_tile_coords_for_bbox(bbox, zoom, limit=16)
    if not sample_coords:
        return None
    try:
        token = await asyncio.to_thread(legacy_get_token)
    except Exception:
        return None

    best_probe: dict[str, Any] | None = None
    for source_date, widen_window in _runtime_probe_attempt_candidates(
        layer=layer,
        display_date=display_date,
        fallback_metadata=fallback_metadata,
    ):
        visible_tiles = 0
        good_tiles = 0
        visible_pct_total = 0.0
        for sample_x, sample_y in sample_coords:
            try:
                content = await _fetch_temporal_tile_attempt(
                    layer=layer,
                    z=zoom,
                    x=sample_x,
                    y=sample_y,
                    display_date=display_date,
                    source_date=source_date,
                    widen_window=widen_window,
                    token=token,
                    frame_role="primary",
                    source_metadata=fallback_metadata,
                    scope=scope,
                    unit_id=unit_id,
                    department=department,
                    scope_type=scope_type,
                    scope_ref=scope_ref,
                    bbox_bucket=bbox_bucket,
                )
            except Exception:
                content = None
            if not content:
                continue
            metrics = _tile_quality_metrics(content, layer=layer)
            if metrics["visually_empty"]:
                continue
            visible_tiles += 1
            visible_pct_total += float(metrics["visible_pct"] or 0.0)
            if metrics["good_enough"]:
                good_tiles += 1
        if not visible_tiles:
            continue
        sample_coverage_ratio = visible_tiles / max(len(sample_coords), 1)
        good_tile_ratio = good_tiles / max(len(sample_coords), 1)
        avg_visible_pct = visible_pct_total / max(visible_tiles, 1)
        score = (good_tile_ratio * 1400.0) + (sample_coverage_ratio * 600.0) + avg_visible_pct - (12.0 if widen_window else 0.0)
        probe_payload = {
            "source_date": source_date,
            "widen_window": widen_window,
            "sample_coverage_ratio": sample_coverage_ratio,
            "good_tile_ratio": good_tile_ratio,
            "avg_visible_pct": avg_visible_pct,
            "good_tiles": good_tiles,
            "score": score,
        }
        if best_probe is None or probe_payload["score"] > best_probe["score"]:
            best_probe = probe_payload

    if best_probe is None:
        return None

    min_ratio = RUNTIME_PROBE_MIN_SAMPLE_COVERAGE_RATIO.get(layer, 0.68)
    min_visible = RUNTIME_PROBE_MIN_AVG_VISIBLE_PCT.get(layer, 6.0)
    min_good_ratio = RUNTIME_PROBE_MIN_GOOD_TILE_RATIO.get(layer, min_ratio)
    if str(scope_type or "") == "nacional":
        if layer in {"rgb", "ndvi", "ndmi", "ndwi", "savi"}:
            min_good_ratio = max(min_good_ratio, 0.84)
        elif layer == "alerta_fusion":
            min_good_ratio = max(min_good_ratio, 0.82)
    if (
        best_probe["sample_coverage_ratio"] < min_ratio
        or best_probe["good_tile_ratio"] < min_good_ratio
        or best_probe["avg_visible_pct"] < min_visible
    ):
        return None

    source_date = best_probe["source_date"]
    public_id = _public_layer_id(layer)
    is_interpolated = source_date != display_date
    selection_reason = "runtime_bucket_carry_forward" if layer == "alerta_fusion" and source_date < display_date else "runtime_bucket_probe"
    renderable_pct = round(float(best_probe["good_tile_ratio"] * 100.0), 2)
    alerta_has_current_optical_support = layer != "alerta_fusion" or (source_date == display_date and not is_interpolated)
    metadata = {
        "layer_id": public_id,
        "available": True,
        "availability": "runtime_bucket_probe",
        "is_interpolated": is_interpolated,
        "primary_source_date": source_date.isoformat(),
        "secondary_source_date": None,
        "blend_weight": 0.0,
        "label": "Interpolado" if is_interpolated else "Real",
        "valid_pixel_pct": renderable_pct,
        "cloud_pixel_pct": 0.0,
        "renderable_pixel_pct": renderable_pct,
        "visual_empty": False,
        "visual_state": "interpolated" if is_interpolated else "ready",
        "skip_in_playback": False,
        "empty_reason": None,
        "selection_reason": selection_reason,
        "coverage_origin": "runtime_bucket_probe",
        "resolved_source_date": source_date.isoformat(),
        "resolved_from_cache": False,
        "source_locked": True,
        "fusion_mode": "s1_s2_carry_forward" if layer == "alerta_fusion" and source_date < display_date else ("s1_s2" if layer == "alerta_fusion" else None),
        "s1_present": layer == "alerta_fusion",
        "s2_present": alerta_has_current_optical_support,
        "s2_mask_valid": alerta_has_current_optical_support,
        "sample_coverage_ratio": round(float(best_probe["sample_coverage_ratio"]), 4),
        "good_tile_ratio": round(float(best_probe["good_tile_ratio"]), 4),
        "sample_visible_pct_avg": round(float(best_probe["avg_visible_pct"]), 2),
        "probe_good_tiles": int(best_probe["good_tiles"]),
        "probe_version": RUNTIME_BUCKET_PROBE_VERSION,
        "bbox_bucket": bbox_bucket,
        "scope_type": scope_type,
        "scope_ref": scope_ref,
    }
    return metadata


def _carry_forward_runtime_metadata(
    *,
    layer: str,
    display_date: date,
    metadata: dict[str, Any],
    carry_from_date: date,
) -> dict[str, Any]:
    carried = dict(metadata or {})
    public_id = _public_layer_id(layer)
    resolved_source_date = str(carried.get("resolved_source_date") or carried.get("primary_source_date") or carry_from_date.isoformat())
    carried["layer_id"] = public_id
    carried["available"] = True
    carried["availability"] = "runtime_bucket_carry_forward"
    carried["is_interpolated"] = True
    carried["label"] = "Interpolado"
    carried["visual_empty"] = False
    carried["visual_state"] = "interpolated"
    carried["skip_in_playback"] = False
    carried["empty_reason"] = None
    carried["selection_reason"] = "runtime_bucket_carry_forward"
    carried["coverage_origin"] = str(carried.get("coverage_origin") or "runtime_bucket_probe")
    carried["resolved_source_date"] = resolved_source_date
    carried["primary_source_date"] = resolved_source_date
    carried["secondary_source_date"] = None
    carried["blend_weight"] = 0.0
    carried["source_locked"] = True
    carried["carry_forward_from_display_date"] = carry_from_date.isoformat()
    return carried


def _temporal_tile_source_candidates(
    *,
    layer: str,
    effective_date: date,
    frame_role: str | None,
    source_metadata: dict[str, Any],
) -> list[date]:
    candidates: list[date] = []
    seen: set[date] = set()

    def _push(value: date | str | None) -> None:
        if value is None:
            return
        resolved = value
        if isinstance(resolved, str):
            try:
                resolved = date.fromisoformat(resolved)
            except Exception:
                return
        if not isinstance(resolved, date):
            return
        resolved = _effective_source_date(resolved)
        if resolved in seen:
            return
        seen.add(resolved)
        candidates.append(resolved)

    primary_source_date = source_metadata.get("primary_source_date")
    secondary_source_date = source_metadata.get("secondary_source_date")
    resolved_source_date = source_metadata.get("resolved_source_date")
    selection_reason = str(source_metadata.get("selection_reason") or "")
    is_interpolated = bool(source_metadata.get("is_interpolated"))
    source_locked = _metadata_bool(source_metadata, "source_locked") or selection_reason != "heuristic_fallback"
    if _is_runtime_resolved_source(source_metadata) or source_locked:
        if frame_role == "secondary":
            _push(secondary_source_date)
        _push(resolved_source_date)
        _push(primary_source_date)
        if not candidates:
            _push(effective_date)
        return candidates or [effective_date]
    prefer_display_date = selection_reason == "heuristic_fallback" and layer in {"rgb", "ndvi", "ndmi", "ndwi", "savi"}

    if layer == "alerta_fusion" and selection_reason == "heuristic_fallback":
        for offset in (1, 2, 3, 4):
            _push(effective_date - timedelta(days=offset))

    if prefer_display_date:
        _push(effective_date)
    if frame_role == "secondary":
        _push(secondary_source_date)
        _push(primary_source_date)
    else:
        _push(primary_source_date)
        _push(secondary_source_date)
    if not prefer_display_date:
        _push(effective_date)

    if selection_reason == "heuristic_fallback" or is_interpolated:
        revisit_days = max(int(TEMPORAL_LAYER_CONFIGS.get(layer, {}).get("revisit_days", 1)), 1)
        for offset in (1, 2):
            _push(effective_date - timedelta(days=revisit_days * offset))
            _push(effective_date + timedelta(days=revisit_days * offset))

    if CAPAS_INFO.get(layer, {}).get("clouds"):
        for offset in (1, 2, 3, 4):
            _push(effective_date - timedelta(days=offset))
            _push(effective_date + timedelta(days=offset))

    return candidates or [effective_date]


def _temporal_tile_request_attempts(
    *,
    layer: str,
    effective_date: date,
    frame_role: str | None,
    source_metadata: dict[str, Any],
) -> list[tuple[date, bool]]:
    attempts: list[tuple[date, bool]] = []
    seen: set[tuple[date, bool]] = set()

    def _push(value: date, widen_window: bool = False) -> None:
        key = (value, widen_window)
        if key in seen:
            return
        seen.add(key)
        attempts.append(key)

    for candidate in _temporal_tile_source_candidates(
        layer=layer,
        effective_date=effective_date,
        frame_role=frame_role,
        source_metadata=source_metadata,
    ):
        _push(candidate, False)

    selection_reason = str(source_metadata.get("selection_reason") or "")
    is_interpolated = bool(source_metadata.get("is_interpolated"))
    source_locked = _metadata_bool(source_metadata, "source_locked") or selection_reason != "heuristic_fallback"
    if (_is_runtime_resolved_source(source_metadata) or source_locked) and CAPAS_INFO.get(layer, {}).get("clouds"):
        for candidate in _temporal_tile_source_candidates(
            layer=layer,
            effective_date=effective_date,
            frame_role=frame_role,
            source_metadata=source_metadata,
        ):
            _push(candidate, True)
        return attempts
    if CAPAS_INFO.get(layer, {}).get("clouds") and (selection_reason == "heuristic_fallback" or is_interpolated):
        for candidate in _temporal_tile_source_candidates(
            layer=layer,
            effective_date=effective_date,
            frame_role=frame_role,
            source_metadata=source_metadata,
        )[:3]:
            _push(candidate, True)

    return attempts


def _time_range_for_temporal_attempt(layer: str, target_date: date, *, widen_window: bool = False) -> tuple[date, date]:
    if not widen_window:
        return _time_range_for_temporal_layer(layer, target_date)
    config = TEMPORAL_LAYER_CONFIGS.get(layer) or {}
    revisit_days = max(int(config.get("revisit_days", 1)), 1)
    before_days = max(int(config.get("window_before_days", 0)), revisit_days, 7)
    after_days = max(int(config.get("window_after_days", 0)), revisit_days, 7)
    if config.get("time_mode") == "carry_forward":
        return target_date - timedelta(days=before_days), target_date
    return target_date - timedelta(days=before_days), target_date + timedelta(days=after_days)


def _timeline_source_cache_key(
    layer: str,
    display_date: date,
    *,
    bbox_bucket: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> str:
    return "::".join(
        [
            layer,
            display_date.isoformat(),
            str(bbox_bucket or "auto"),
            str(scope_type or "auto"),
            str(scope_ref or "global"),
        ]
    )


def _metadata_float(metadata_extra: dict[str, Any], key: str, default: float | None = None) -> float | None:
    try:
        value = metadata_extra.get(key)
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _metadata_bool(metadata_extra: dict[str, Any], key: str, default: bool = False) -> bool:
    value = metadata_extra.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if value is None:
        return default
    return bool(value)


def _renderable_pixel_pct(metadata_extra: dict[str, Any], availability_score: float) -> float:
    renderable = _metadata_float(metadata_extra, "renderable_pixel_pct")
    if renderable is not None:
        return max(0.0, min(100.0, renderable))
    valid_pixels = _metadata_float(metadata_extra, "valid_pixel_pct")
    if valid_pixels is not None:
        return max(0.0, min(100.0, valid_pixels))
    if availability_score <= 0:
        return 0.0
    return 100.0


def _snapshot_match_score(
    row: dict[str, Any],
    *,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    bbox_bucket: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> float:
    score = float(row.get("availability_score") or 0.0)
    metadata_extra = row.get("metadata_extra") or {}
    if unit_id and row.get("unit_id") == unit_id:
        score += 500.0
    if department and str(row.get("department") or "").lower() == str(department).lower():
        score += 180.0
    if scope and row.get("scope") == scope:
        score += 120.0
    if scope_type and str(metadata_extra.get("scope_type") or "") == str(scope_type):
        score += 40.0
    if scope_ref and str(metadata_extra.get("scope_ref") or "") == str(scope_ref):
        score += 60.0
    if bbox_bucket and str(metadata_extra.get("bbox_bucket") or "") == str(bbox_bucket):
        score += 25.0
    return score


def _snapshot_row_matches_request(
    row: dict[str, Any],
    *,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    bbox_bucket: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> bool:
    metadata_extra = row.get("metadata_extra") or {}
    row_scope = str(row.get("scope") or "")
    row_unit_id = str(row.get("unit_id") or "")
    row_department = str(row.get("department") or "")
    row_scope_type = str(metadata_extra.get("scope_type") or "")
    row_scope_ref = str(metadata_extra.get("scope_ref") or "")
    row_bbox_bucket = str(metadata_extra.get("bbox_bucket") or "")

    if unit_id:
        if row_unit_id != str(unit_id) and row_scope_ref != str(scope_ref or unit_id):
            return False
    elif department:
        expected_department = str(department).lower()
        if row_department.lower() != expected_department and row_scope_ref.lower() != str(scope_ref or department).lower():
            return False
    elif scope_type or scope:
        expected_scope_type = str(scope_type or scope or "")
        expected_scope_ref = str(scope_ref or scope or "")
        if expected_scope_type == "nacional":
            if row_scope not in {"nacional"} and row_scope_type not in {"nacional"} and row_scope_ref.lower() not in {"uruguay", "nacional"}:
                return False
        else:
            if row_scope_type and row_scope_type != expected_scope_type and row_scope != expected_scope_type:
                return False
            if expected_scope_ref and row_scope_ref and row_scope_ref != expected_scope_ref:
                return False

    if bbox_bucket and row_bbox_bucket and row_bbox_bucket != str(bbox_bucket):
        return False
    return True


def _select_snapshot_row(
    rows: list[dict[str, Any]] | None,
    *,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    bbox_bucket: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, Any] | None:
    if not rows:
        return None
    filtered = [
        row
        for row in rows
        if _snapshot_row_matches_request(
            row,
            scope=scope,
            unit_id=unit_id,
            department=department,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )
    ]
    if not filtered:
        return None
    return max(
        filtered,
        key=lambda row: _snapshot_match_score(
            row,
            scope=scope,
            unit_id=unit_id,
            department=department,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=scope_ref,
        ),
    )


def _frame_visual_state(
    *,
    availability: str,
    is_interpolated: bool,
    visual_empty: bool,
    cache_status: str | None = None,
) -> str:
    if visual_empty:
        return "empty"
    if availability == "missing":
        return "missing"
    if cache_status == "warming":
        return "warming"
    if is_interpolated:
        return "interpolated"
    return "ready"


def _build_frame_signature(
    *,
    layer_id: str,
    display_date: str,
    resolved_source_date: str | None,
    visual_state: str,
    selection_reason: str,
    source_locked: bool,
    cache_status: str | None = None,
    empty_reason: str | None = None,
    fusion_mode: str | None = None,
) -> str:
    raw = "|".join(
        [
            str(layer_id or ""),
            str(display_date or ""),
            str(resolved_source_date or ""),
            str(visual_state or ""),
            str(selection_reason or ""),
            "1" if source_locked else "0",
            str(cache_status or ""),
            str(empty_reason or ""),
            str(fusion_mode or ""),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _frame_payload(
    *,
    public_id: str,
    display_date: str,
    availability: str,
    is_interpolated: bool,
    primary_source_date: str,
    secondary_source_date: str | None,
    blend_weight: float,
    label: str,
    metadata_extra: dict[str, Any],
    availability_score: float,
    selection_reason: str,
    cache_status: str | None = None,
) -> dict[str, Any]:
    renderable_pct = _renderable_pixel_pct(metadata_extra, availability_score)
    valid_pixel_pct = _metadata_float(metadata_extra, "valid_pixel_pct", renderable_pct) or 0.0
    cloud_pixel_pct = _metadata_float(metadata_extra, "cloud_pixel_pct", 0.0) or 0.0
    visual_empty = _metadata_bool(metadata_extra, "visual_empty") or (
        availability == "missing" or renderable_pct < VISUAL_EMPTY_RENDERABLE_THRESHOLD_PCT
    )
    empty_reason = None
    if availability == "missing":
        empty_reason = str(metadata_extra.get("empty_reason") or "missing_snapshot")
    elif visual_empty:
        empty_reason = str(metadata_extra.get("empty_reason") or "low_renderable_coverage")
    fusion_mode = metadata_extra.get("fusion_mode")
    if public_id == "alerta" and not fusion_mode and not visual_empty and availability != "missing":
        if availability in {"historical_carry_forward", "historical_previous_only", "historical_forward_fill"}:
            fusion_mode = "s1_s2_carry_forward"
        else:
            fusion_mode = "s1_s2"
    resolved_source_date = metadata_extra.get("resolved_source_date") or primary_source_date
    source_locked = _metadata_bool(metadata_extra, "source_locked", selection_reason != "heuristic_fallback")
    visual_state = _frame_visual_state(
        availability=availability,
        is_interpolated=is_interpolated,
        visual_empty=visual_empty,
        cache_status=cache_status,
    )
    effective_cache_status = str(cache_status or metadata_extra.get("cache_status") or ("empty" if visual_empty else "ready"))
    warm_available = effective_cache_status == "ready" and visual_state in {"ready", "interpolated"} and not visual_empty
    frame_signature = _build_frame_signature(
        layer_id=public_id,
        display_date=display_date,
        resolved_source_date=str(resolved_source_date) if resolved_source_date else None,
        visual_state=visual_state,
        selection_reason=str(metadata_extra.get("selection_reason") or selection_reason),
        source_locked=source_locked,
        cache_status=effective_cache_status,
        empty_reason=empty_reason,
        fusion_mode=str(fusion_mode) if fusion_mode else None,
    )
    return {
        "layer_id": public_id,
        "availability": availability,
        "available": visual_state in {"ready", "interpolated"},
        "display_date": display_date,
        "is_interpolated": is_interpolated,
        "primary_source_date": primary_source_date,
        "secondary_source_date": secondary_source_date,
        "blend_weight": blend_weight,
        "label": label,
        "valid_pixel_pct": round(valid_pixel_pct, 2),
        "cloud_pixel_pct": round(cloud_pixel_pct, 2),
        "renderable_pixel_pct": round(renderable_pct, 2),
        "visual_empty": visual_empty,
        "visual_state": visual_state,
        "skip_in_playback": visual_state in {"warming", "empty", "missing"},
        "empty_reason": empty_reason,
        "selection_reason": str(metadata_extra.get("selection_reason") or selection_reason),
        "coverage_origin": str(metadata_extra.get("coverage_origin") or selection_reason),
        "resolved_source_date": str(resolved_source_date) if resolved_source_date else None,
        "resolved_from_cache": _metadata_bool(metadata_extra, "resolved_from_cache"),
        "source_locked": source_locked,
        "frame_signature": frame_signature,
        "fusion_mode": fusion_mode,
        "s1_present": _metadata_bool(metadata_extra, "s1_present", public_id == "alerta"),
        "s2_present": _metadata_bool(metadata_extra, "s2_present", public_id != "alerta" or (availability != "missing" and not visual_empty)),
        "s2_mask_valid": _metadata_bool(metadata_extra, "s2_mask_valid", public_id != "alerta" or (availability != "missing" and not visual_empty)),
        "cache_status": effective_cache_status,
        "warm_available": warm_available,
    }


def _missing_frame_metadata_from_heuristic(
    *,
    layer: str,
    display_date: date,
    fallback_metadata: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    public_id = _public_layer_id(layer)
    primary_source_date = str(fallback_metadata.get("primary_source_date") or display_date.isoformat())
    frame_signature = _build_frame_signature(
        layer_id=public_id,
        display_date=display_date.isoformat(),
        resolved_source_date=None,
        visual_state="missing",
        selection_reason=str(fallback_metadata.get("selection_reason") or "heuristic_fallback"),
        source_locked=True,
        cache_status="missing",
        empty_reason=reason,
        fusion_mode=None if public_id == "alerta" else str(fallback_metadata.get("fusion_mode") or ""),
    )
    return {
        **fallback_metadata,
        "layer_id": public_id,
        "display_date": display_date.isoformat(),
        "available": False,
        "availability": "missing",
        "is_interpolated": False,
        "primary_source_date": primary_source_date,
        "secondary_source_date": None,
        "blend_weight": 0.0,
        "label": "Sin cobertura",
        "valid_pixel_pct": 0.0,
        "cloud_pixel_pct": 0.0,
        "renderable_pixel_pct": 0.0,
        "visual_empty": True,
        "visual_state": "missing",
        "skip_in_playback": True,
        "empty_reason": reason,
        "selection_reason": str(fallback_metadata.get("selection_reason") or "heuristic_fallback"),
        "coverage_origin": "missing_bucket_metadata",
        "resolved_source_date": None,
        "resolved_from_cache": False,
        "source_locked": True,
        "frame_signature": frame_signature,
        "fusion_mode": None if public_id == "alerta" else fallback_metadata.get("fusion_mode"),
        "s1_present": False,
        "s2_present": False,
        "s2_mask_valid": False,
        "cache_status": "missing",
        "warm_available": False,
    }


def _frame_metadata_from_canonical_product(
    *,
    layer: str,
    display_date: date,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    public_id = _public_layer_id(layer)
    visual_state = str(metadata.get("visual_state") or ("empty" if metadata.get("visual_empty") else "ready"))
    resolved_source_date = str(metadata.get("resolved_source_date") or metadata.get("source_date") or display_date.isoformat())
    cache_status = str(metadata.get("cache_status") or ("empty" if metadata.get("visual_empty") else "ready"))
    renderable_pct = float(metadata.get("renderable_pixel_pct") or metadata.get("visible_pixel_pct") or 0.0)
    visual_empty = bool(metadata.get("visual_empty")) or visual_state in {"empty", "missing"}
    empty_reason = str(metadata.get("empty_reason") or ("internal_product_empty" if visual_empty else ""))
    frame_signature = _build_frame_signature(
        layer_id=public_id,
        display_date=display_date.isoformat(),
        resolved_source_date=resolved_source_date,
        visual_state=visual_state,
        selection_reason=str(metadata.get("selection_reason") or metadata.get("coverage_origin") or "internal_product"),
        source_locked=True,
        cache_status=cache_status,
        empty_reason=empty_reason if empty_reason else None,
        fusion_mode=str(metadata.get("fusion_mode") or ""),
    )
    return {
        "layer_id": public_id,
        "available": visual_state in {"ready", "interpolated"},
        "availability": "product_ready" if visual_state not in {"empty", "missing"} else "missing",
        "is_interpolated": False,
        "primary_source_date": resolved_source_date,
        "secondary_source_date": None,
        "blend_weight": 0.0,
        "label": "Producto interno",
        "valid_pixel_pct": round(float(metadata.get("valid_pixel_pct") or renderable_pct), 2),
        "cloud_pixel_pct": round(float(metadata.get("cloud_pixel_pct") or 0.0), 2),
        "renderable_pixel_pct": round(renderable_pct, 2),
        "visual_empty": visual_empty,
        "visual_state": visual_state,
        "skip_in_playback": visual_state in {"warming", "empty", "missing"},
        "empty_reason": empty_reason if empty_reason else None,
        "selection_reason": str(metadata.get("selection_reason") or metadata.get("coverage_origin") or "internal_product"),
        "coverage_origin": str(metadata.get("coverage_origin") or "internal_product"),
        "resolved_source_date": resolved_source_date,
        "resolved_from_cache": True,
        "source_locked": True,
        "frame_signature": frame_signature,
        "fusion_mode": metadata.get("fusion_mode"),
        "s1_present": _metadata_bool(metadata, "s1_present", public_id == "alerta"),
        "s2_present": _metadata_bool(metadata, "s2_present", public_id != "alerta" or not visual_empty),
        "s2_mask_valid": _metadata_bool(metadata, "s2_mask_valid", public_id != "alerta" or not visual_empty),
        "cache_status": cache_status,
        "warm_available": cache_status == "ready" and visual_state in {"ready", "interpolated"} and not visual_empty,
    }


async def _load_persisted_timeline_source_metadata(
    *,
    layer: str,
    display_date: date,
    bbox_bucket: str | None,
    scope_type: str | None,
    scope_ref: str | None,
) -> dict[str, Any] | None:
    cache_key = raster_cache_key(
        cache_kind="timeline_source",
        layer_id=_public_layer_id(layer),
        display_date=display_date,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key).limit(1))
            row = result.scalar_one_or_none()
    except Exception:
        return None
    if row is None or not isinstance(row.metadata_extra, dict):
        return None
    metadata = dict(row.metadata_extra)
    if not _is_persistable_timeline_source_metadata(metadata):
        return None
    metadata.setdefault("cache_status", row.status or "missing")
    metadata.setdefault("selection_reason", metadata.get("selection_reason") or "runtime_bucket_probe")
    metadata.setdefault("coverage_origin", metadata.get("coverage_origin") or "persisted_timeline_source")
    metadata["resolved_from_cache"] = True
    return metadata


async def _load_persisted_timeline_source_metadata_index(
    *,
    layers: list[str],
    date_from: date,
    date_to: date,
    bbox_bucket: str | None,
    scope_type: str | None,
    scope_ref: str | None,
) -> dict[str, dict[str, dict[str, Any]]]:
    if not layers:
        return {}
    public_layers = sorted({_public_layer_id(layer) for layer in layers})
    start_at = datetime.combine(date_from, datetime.min.time(), tzinfo=timezone.utc)
    end_at = datetime.combine(date_to + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    try:
        async with AsyncSessionLocal() as session:
            query = (
                select(
                    RasterCacheEntry.layer_id,
                    RasterCacheEntry.display_date,
                    RasterCacheEntry.metadata_extra,
                    RasterCacheEntry.status,
                    RasterCacheEntry.updated_at,
                )
                .where(
                    RasterCacheEntry.cache_kind == "timeline_source",
                    RasterCacheEntry.layer_id.in_(public_layers),
                    RasterCacheEntry.display_date >= start_at,
                    RasterCacheEntry.display_date < end_at,
                )
                .order_by(
                    RasterCacheEntry.layer_id,
                    RasterCacheEntry.display_date,
                    RasterCacheEntry.updated_at.desc(),
                )
            )
            if bbox_bucket is not None:
                query = query.where(RasterCacheEntry.bbox_bucket == bbox_bucket)
            if scope_type is not None:
                query = query.where(RasterCacheEntry.scope_type == scope_type)
            if scope_ref is not None:
                query = query.where(RasterCacheEntry.scope_ref == scope_ref)
            result = await session.execute(query)
            rows = result.all()
    except Exception:
        return {}

    index: dict[str, dict[str, dict[str, Any]]] = {}
    for layer_id, display_date_value, metadata_extra, status, _updated_at in rows:
        if display_date_value is None or not isinstance(metadata_extra, dict):
            continue
        display_date_key = display_date_value.date().isoformat()
        layer_bucket = index.setdefault(str(layer_id), {})
        if display_date_key in layer_bucket:
            continue
        metadata = dict(metadata_extra)
        if not _is_persistable_timeline_source_metadata(metadata):
            continue
        metadata.setdefault("cache_status", status or "missing")
        metadata.setdefault("selection_reason", metadata.get("selection_reason") or "runtime_bucket_probe")
        metadata.setdefault("coverage_origin", metadata.get("coverage_origin") or "persisted_timeline_source")
        metadata["resolved_from_cache"] = True
        layer_bucket[display_date_key] = metadata
    return index


async def _persist_timeline_source_metadata(
    *,
    layer: str,
    display_date: date,
    bbox_bucket: str | None,
    scope_type: str | None,
    scope_ref: str | None,
    metadata: dict[str, Any],
) -> None:
    if not _is_persistable_timeline_source_metadata(metadata):
        return
    try:
        async with AsyncSessionLocal() as session:
            await upsert_raster_cache_entry(
                session,
                cache_key=raster_cache_key(
                    cache_kind="timeline_source",
                    layer_id=_public_layer_id(layer),
                    display_date=display_date,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=scope_ref,
                ),
                layer_id=_public_layer_id(layer),
                cache_kind="timeline_source",
                scope_type=scope_type,
                scope_ref=scope_ref,
                display_date=display_date,
                source_date=metadata.get("primary_source_date"),
                bbox_bucket=bbox_bucket,
                status=_timeline_source_status(metadata),
                metadata_extra=metadata,
                last_hit_at=datetime.now(timezone.utc),
            )
            await session.commit()
    except Exception:
        return


async def _record_temporal_tile_cache_result(
    *,
    layer: str,
    display_date: date,
    source_date: date | str | None,
    zoom: int,
    bbox_bucket: str | None,
    scope_type: str | None,
    scope_ref: str | None,
    content: bytes,
    frame_metadata: dict[str, Any] | None,
    status: str | None = None,
) -> None:
    metrics = _tile_quality_metrics(content, layer=layer)
    incoming_metadata = dict(frame_metadata or {})
    incoming_metadata.setdefault("resolved_source_date", str(source_date) if source_date is not None else None)
    incoming_metadata.setdefault("coverage_origin", incoming_metadata.get("coverage_origin") or "tile_fetch")
    incoming_metadata["tile_visible_pct"] = round(float(metrics["visible_pct"]), 2)
    incoming_metadata["tile_texture_score"] = round(float(metrics["texture_score"]), 2)
    incoming_metadata["cloudlike_pct"] = round(float(metrics["cloudlike_pct"]), 2)
    incoming_metadata["cloudlike_max_tile_pct"] = round(float(metrics["cloudlike_max_tile_pct"]), 2)
    incoming_metadata["visual_empty"] = bool(metrics["visually_empty"])
    incoming_metadata["renderable_pixel_pct"] = round(float(metrics["visible_pct"]), 2)
    resolved_status = status or ("empty" if metrics["visually_empty"] else "ready")
    try:
        async with AsyncSessionLocal() as session:
            cache_key = raster_cache_key(
                cache_kind="analytic_tile",
                layer_id=_public_layer_id(layer),
                display_date=display_date,
                source_date=source_date,
                zoom=zoom,
                bbox_bucket=bbox_bucket,
                scope_type=scope_type,
                scope_ref=scope_ref,
            )
            result = await session.execute(select(RasterCacheEntry).where(RasterCacheEntry.cache_key == cache_key).limit(1))
            existing_row = result.scalar_one_or_none()
            existing_metadata = dict(existing_row.metadata_extra) if existing_row and isinstance(existing_row.metadata_extra, dict) else {}
            existing_status = str(existing_row.status or "")
            if existing_metadata.get("visual_empty") is False and resolved_status == "empty":
                resolved_status = existing_status or "ready"
            merged_metadata = {**incoming_metadata, **existing_metadata}
            merged_metadata["visual_empty"] = bool(existing_metadata.get("visual_empty")) and bool(metrics["visually_empty"]) if existing_row else bool(metrics["visually_empty"])
            merged_metadata["renderable_pixel_pct"] = round(
                max(
                    float(existing_metadata.get("renderable_pixel_pct") or 0.0),
                    float(incoming_metadata.get("renderable_pixel_pct") or 0.0),
                ),
                2,
            )
            if existing_status == "ready" and resolved_status == "empty":
                resolved_status = "ready"
            await upsert_raster_cache_entry(
                session,
                cache_key=cache_key,
                layer_id=_public_layer_id(layer),
                cache_kind="analytic_tile",
                scope_type=scope_type,
                scope_ref=scope_ref,
                display_date=display_date,
                source_date=source_date,
                zoom=zoom,
                bbox_bucket=bbox_bucket,
                storage_backend="filesystem",
                storage_key=f"temporal/{_public_layer_id(layer)}/{display_date.isoformat()}/{zoom}/{bbox_bucket or 'auto'}.png",
                status=resolved_status,
                bytes_size=len(content or b""),
                metadata_extra=merged_metadata,
                last_hit_at=datetime.now(timezone.utc),
            )
            await session.commit()
    except Exception:
        return


def _timeline_snapshot_padding(layer: str) -> tuple[int, int]:
    config = TEMPORAL_LAYER_CONFIGS[layer]
    revisit_days = int(config.get("revisit_days", 1))
    before_days = max(int(config.get("window_before_days", 0)), revisit_days * 2, 7)
    after_days = max(int(config.get("window_after_days", 0)), revisit_days * 2, 7)
    return before_days, after_days


async def _load_timeline_snapshot_index(
    *,
    layers: list[str],
    date_from: date,
    date_to: date,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
) -> dict[str, dict[date, list[dict[str, Any]]]]:
    if not layers:
        return {}
    before_padding = max((_timeline_snapshot_padding(layer)[0] for layer in layers), default=7)
    after_padding = max((_timeline_snapshot_padding(layer)[1] for layer in layers), default=7)
    start_at = datetime.combine(date_from - timedelta(days=before_padding), datetime.min.time(), tzinfo=timezone.utc)
    end_at = datetime.combine(date_to + timedelta(days=after_padding + 1), datetime.min.time(), tzinfo=timezone.utc)
    try:
        async with AsyncSessionLocal() as session:
            query = (
                select(
                    SatelliteLayerSnapshot.layer_key,
                    SatelliteLayerSnapshot.observed_at,
                    SatelliteLayerSnapshot.metadata_extra,
                    SatelliteLayerSnapshot.availability_score,
                    SatelliteLayerSnapshot.unit_id,
                    SatelliteLayerSnapshot.scope,
                    SatelliteLayerSnapshot.department,
                )
                .where(
                    SatelliteLayerSnapshot.layer_key.in_(layers),
                    SatelliteLayerSnapshot.observed_at >= start_at,
                    SatelliteLayerSnapshot.observed_at < end_at,
                )
            )
            if unit_id:
                query = query.where(SatelliteLayerSnapshot.unit_id == unit_id)
            elif department:
                query = query.where(SatelliteLayerSnapshot.department == department)
            elif scope:
                query = query.where(SatelliteLayerSnapshot.scope == scope)
            result = await session.execute(
                query.order_by(
                    SatelliteLayerSnapshot.layer_key,
                    SatelliteLayerSnapshot.observed_at.desc(),
                    SatelliteLayerSnapshot.availability_score.desc(),
                )
            )
            rows = result.all()
    except Exception:
        return {layer: {} for layer in layers}

    index: dict[str, dict[date, list[dict[str, Any]]]] = {layer: {} for layer in layers}
    for layer_key, observed_at, metadata_extra, availability_score, snapshot_unit_id, snapshot_scope, snapshot_department in rows:
        if observed_at is None:
            continue
        observed_date = observed_at.date()
        layer_bucket = index.setdefault(layer_key, {})
        layer_bucket.setdefault(observed_date, []).append(
            {
            "observed_date": observed_date,
            "metadata_extra": metadata_extra or {},
            "availability_score": float(availability_score or 0.0),
            "unit_id": snapshot_unit_id,
            "scope": snapshot_scope,
            "department": snapshot_department,
            }
        )
    return index


def _frame_metadata_from_snapshot(
    *,
    internal_layer: str,
    display_date: date,
    snapshot_index: dict[str, dict[date, list[dict[str, Any]]]],
    bbox_bucket: str | None = None,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, Any]:
    config = TEMPORAL_LAYER_CONFIGS[internal_layer]
    public_id = str(config["public_id"])
    layer_rows = snapshot_index.get(internal_layer) or {}
    selected_rows_by_date: dict[date, dict[str, Any]] = {}
    exact = _select_snapshot_row(
        layer_rows.get(display_date),
        scope=scope,
        unit_id=unit_id,
        department=department,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    if exact is not None:
        metadata_extra = exact.get("metadata_extra") or {}
        primary_source_date = metadata_extra.get("primary_source_date") or display_date.isoformat()
        secondary_source_date = metadata_extra.get("secondary_source_date")
        blend_weight = float(metadata_extra.get("blend_weight") or 0.0)
        is_interpolated = bool(metadata_extra.get("is_interpolated")) or bool(secondary_source_date)
        label = metadata_extra.get("label") or ("Interpolado" if is_interpolated else "Real")
        availability = metadata_extra.get("availability") or ("available" if exact.get("availability_score", 0.0) > 0 else "missing")
        return _frame_payload(
            public_id=public_id,
            display_date=display_date.isoformat(),
            availability=availability,
            is_interpolated=is_interpolated,
            primary_source_date=str(primary_source_date),
            secondary_source_date=str(secondary_source_date) if secondary_source_date else None,
            blend_weight=blend_weight,
            label=label,
            metadata_extra=metadata_extra,
            availability_score=float(exact.get("availability_score") or 0.0),
            selection_reason="snapshot_exact",
        )

    for observed_date, rows in layer_rows.items():
        selected_row = _select_snapshot_row(
            rows,
            scope=scope,
            unit_id=unit_id,
            department=department,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )
        if selected_row is not None:
            selected_rows_by_date[observed_date] = selected_row

    available_dates = sorted(selected_rows_by_date.keys())
    previous_date = max((item for item in available_dates if item < display_date), default=None)
    next_date = min((item for item in available_dates if item > display_date), default=None)
    time_mode = str(config.get("time_mode") or "symmetric")

    if time_mode == "carry_forward":
        if previous_date is not None:
            previous_row = selected_rows_by_date.get(previous_date)
            previous_meta = (previous_row or {}).get("metadata_extra") or {}
            return _frame_payload(
                public_id=public_id,
                display_date=display_date.isoformat(),
                availability="historical_carry_forward",
                is_interpolated=True,
                primary_source_date=str(previous_meta.get("primary_source_date") or previous_date.isoformat()),
                secondary_source_date=None,
                blend_weight=0.0,
                label="Interpolado",
                metadata_extra=previous_meta,
                availability_score=float((previous_row or {}).get("availability_score") or 0.0),
                selection_reason="carry_forward_previous",
            )
        if next_date is not None:
            next_row = selected_rows_by_date.get(next_date)
            next_meta = (next_row or {}).get("metadata_extra") or {}
            return _frame_payload(
                public_id=public_id,
                display_date=display_date.isoformat(),
                availability="historical_forward_fill",
                is_interpolated=True,
                primary_source_date=str(next_meta.get("primary_source_date") or next_date.isoformat()),
                secondary_source_date=None,
                blend_weight=0.0,
                label="Interpolado",
                metadata_extra=next_meta,
                availability_score=float((next_row or {}).get("availability_score") or 0.0),
                selection_reason="carry_forward_next",
            )

    if previous_date is not None and next_date is not None:
        total_days = max((next_date - previous_date).days, 1)
        previous_row = selected_rows_by_date.get(previous_date)
        next_row = selected_rows_by_date.get(next_date)
        previous_meta = (previous_row or {}).get("metadata_extra") or {}
        next_meta = (next_row or {}).get("metadata_extra") or {}
        merged_meta = {
            **next_meta,
            **previous_meta,
            "secondary_source_date": next_meta.get("primary_source_date") or next_meta.get("secondary_source_date"),
            "blend_weight": round((display_date - previous_date).days / total_days, 3),
            "selection_reason": "historical_blend",
        }
        return _frame_payload(
            public_id=public_id,
            display_date=display_date.isoformat(),
            availability="historical_blend",
            is_interpolated=True,
            primary_source_date=str(previous_meta.get("primary_source_date") or previous_date.isoformat()),
            secondary_source_date=str(next_meta.get("primary_source_date") or next_date.isoformat()),
            blend_weight=round((display_date - previous_date).days / total_days, 3),
            label="Interpolado",
            metadata_extra=merged_meta,
            availability_score=max(
                float((previous_row or {}).get("availability_score") or 0.0),
                float((next_row or {}).get("availability_score") or 0.0),
            ),
            selection_reason="historical_blend",
        )
    if previous_date is not None:
        previous_row = selected_rows_by_date.get(previous_date)
        previous_meta = (previous_row or {}).get("metadata_extra") or {}
        return _frame_payload(
            public_id=public_id,
            display_date=display_date.isoformat(),
            availability="historical_previous_only",
            is_interpolated=True,
            primary_source_date=str(previous_meta.get("primary_source_date") or previous_date.isoformat()),
            secondary_source_date=None,
            blend_weight=0.0,
            label="Interpolado",
            metadata_extra=previous_meta,
            availability_score=float((previous_row or {}).get("availability_score") or 0.0),
            selection_reason="historical_previous_only",
        )
    if next_date is not None:
        next_row = selected_rows_by_date.get(next_date)
        next_meta = (next_row or {}).get("metadata_extra") or {}
        return _frame_payload(
            public_id=public_id,
            display_date=display_date.isoformat(),
            availability="historical_next_only",
            is_interpolated=True,
            primary_source_date=str(next_meta.get("primary_source_date") or next_date.isoformat()),
            secondary_source_date=None,
            blend_weight=0.0,
            label="Interpolado",
            metadata_extra=next_meta,
            availability_score=float((next_row or {}).get("availability_score") or 0.0),
            selection_reason="historical_next_only",
        )

    primary_date, secondary_date, blend_weight, is_interpolated = _anchor_frame_dates(internal_layer, display_date)
    heuristic_meta = {
        "primary_source_date": primary_date.isoformat(),
        "secondary_source_date": secondary_date.isoformat() if secondary_date else None,
        "blend_weight": blend_weight,
        "selection_reason": "heuristic_fallback",
        "renderable_pixel_pct": 0.0,
        "visual_empty": True,
        "empty_reason": "missing_bucket_metadata",
        "resolved_source_date": None,
        "source_locked": True,
    }
    return _missing_frame_metadata_from_heuristic(
        layer=internal_layer,
        display_date=display_date,
        fallback_metadata=heuristic_meta,
        reason="missing_bucket_metadata",
    )


async def _resolve_timeline_source_metadata(
    layer: str,
    display_date: date,
    *,
    bbox_bucket: str | None = None,
    bbox: str | None = None,
    zoom: int | None = None,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    allow_runtime_probe: bool = True,
) -> dict[str, Any]:
    normalized_scope_ref = _normalized_scope_ref(
        scope_type=scope_type,
        scope_ref=scope_ref,
        scope=scope,
        unit_id=unit_id,
        department=department,
    )
    normalized_scope_kind = str(scope_type or scope or "nacional").strip().lower()
    cache_key = _timeline_source_cache_key(
        layer,
        display_date,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    cached = TIMELINE_SOURCE_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < TIMELINE_MANIFEST_CACHE_TTL_SECONDS:
        return cached[1]
    canonical_metadata = await get_canonical_product_frame_metadata(
        layer_id=layer,
        display_date=display_date,
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    if canonical_metadata is not None:
        resolved = _frame_metadata_from_canonical_product(
            layer=layer,
            display_date=display_date,
            metadata=canonical_metadata,
        )
        TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), resolved)
        return resolved
    # Prefer persisted, bucket-specific resolution metadata over legacy snapshot interpolation.
    persisted = await _load_persisted_timeline_source_metadata(
        layer=layer,
        display_date=display_date,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    if persisted is not None:
        TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), persisted)
        return persisted
    snapshot_index = await _load_timeline_snapshot_index(
        layers=[layer],
        date_from=display_date,
        date_to=display_date,
        scope=scope,
        unit_id=unit_id,
        department=department,
    )
    if not (snapshot_index.get(layer) or {}) and (scope or unit_id or department):
        snapshot_index = await _load_timeline_snapshot_index(layers=[layer], date_from=display_date, date_to=display_date)
    metadata = _frame_metadata_from_snapshot(
        internal_layer=layer,
        display_date=display_date,
        snapshot_index=snapshot_index,
        bbox_bucket=bbox_bucket,
        scope=scope,
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    if str(metadata.get("selection_reason") or "") != "heuristic_fallback":
        TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), metadata)
        return metadata
    runtime_probe_enabled = bool(allow_runtime_probe) and not _disable_heuristic_ready_enabled()
    if runtime_probe_enabled and bbox and zoom is not None:
        probed = await _probe_runtime_bucket_source_metadata(
            layer=layer,
            display_date=display_date,
            bbox=bbox,
            zoom=zoom,
            fallback_metadata=metadata,
            scope=scope,
            unit_id=unit_id,
            department=department,
            scope_type=scope_type,
            scope_ref=normalized_scope_ref,
            bbox_bucket=bbox_bucket,
        )
        if probed is not None:
            await _persist_timeline_source_metadata(
                layer=layer,
                display_date=display_date,
                bbox_bucket=bbox_bucket,
                scope_type=scope_type,
                scope_ref=normalized_scope_ref,
                metadata=probed,
            )
            TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), probed)
            return probed
        if CAPAS_INFO.get(layer, {}).get("clouds"):
            revisit_days = max(int(TEMPORAL_LAYER_CONFIGS.get(layer, {}).get("revisit_days", 1)), 1)
            search_days = max(7, revisit_days * 3)
            for offset in range(1, search_days + 1):
                previous_display_date = display_date - timedelta(days=offset)
                previous_probed = await _probe_runtime_bucket_source_metadata(
                    layer=layer,
                    display_date=previous_display_date,
                    bbox=bbox,
                    zoom=zoom,
                    fallback_metadata={"selection_reason": "heuristic_fallback"},
                    scope=scope,
                    unit_id=unit_id,
                    department=department,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    bbox_bucket=bbox_bucket,
                )
                if previous_probed is None:
                    continue
                carried = _carry_forward_runtime_metadata(
                    layer=layer,
                    display_date=display_date,
                    metadata=previous_probed,
                    carry_from_date=previous_display_date,
                )
                await _persist_timeline_source_metadata(
                    layer=layer,
                    display_date=display_date,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    metadata=carried,
                )
                TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), carried)
                return carried
    metadata = _missing_frame_metadata_from_heuristic(
        layer=layer,
        display_date=display_date,
        fallback_metadata=metadata,
        reason="missing_bucket_metadata",
    )
    TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), metadata)
    return metadata


def _runtime_tile_unlock_metadata(
    *,
    layer: str,
    display_date: date,
    source_date: date,
    base_metadata: dict[str, Any],
) -> dict[str, Any]:
    public_id = _public_layer_id(layer)
    is_interpolated = source_date != display_date
    visual_state = "interpolated" if is_interpolated else "ready"
    selection_reason = "runtime_bucket_carry_forward" if is_interpolated else "runtime_bucket_probe"
    fusion_mode = None
    if public_id == "alerta":
        fusion_mode = "s1_s2_carry_forward" if is_interpolated else "s1_s2"
    valid_pixel_pct = _metadata_float(base_metadata, "valid_pixel_pct", None)
    cloud_pixel_pct = _metadata_float(base_metadata, "cloud_pixel_pct", None)
    renderable_pixel_pct = _metadata_float(base_metadata, "renderable_pixel_pct", None)
    frame_signature = _build_frame_signature(
        layer_id=public_id,
        display_date=display_date.isoformat(),
        resolved_source_date=source_date.isoformat(),
        visual_state=visual_state,
        selection_reason=selection_reason,
        source_locked=True,
        cache_status="ready",
        fusion_mode=fusion_mode,
    )
    return {
        **base_metadata,
        "layer_id": public_id,
        "display_date": display_date.isoformat(),
        "available": True,
        "availability": "runtime_bucket_probe",
        "is_interpolated": is_interpolated,
        "primary_source_date": source_date.isoformat(),
        "secondary_source_date": None,
        "blend_weight": 0.0,
        "label": "Interpolado" if is_interpolated else "Real",
        "valid_pixel_pct": float(valid_pixel_pct if valid_pixel_pct is not None else 0.0),
        "cloud_pixel_pct": float(cloud_pixel_pct if cloud_pixel_pct is not None else 0.0),
        "renderable_pixel_pct": float(renderable_pixel_pct if renderable_pixel_pct is not None else 0.0),
        "visual_empty": False,
        "visual_state": visual_state,
        "skip_in_playback": False,
        "empty_reason": None,
        "selection_reason": selection_reason,
        "coverage_origin": "runtime_tile_unlock_fallback",
        "resolved_source_date": source_date.isoformat(),
        "resolved_from_cache": False,
        "source_locked": True,
        "frame_signature": frame_signature,
        "fusion_mode": fusion_mode,
        "s1_present": public_id == "alerta",
        "s2_present": public_id != "alerta" or not is_interpolated,
        "s2_mask_valid": public_id != "alerta" or not is_interpolated,
        "cache_status": "ready",
        "warm_available": True,
    }


def _lock_frame_metadata_to_source_date(
    *,
    layer: str,
    display_date: date,
    source_date: date,
    base_metadata: dict[str, Any],
    frame_signature: str | None = None,
) -> dict[str, Any]:
    public_id = _public_layer_id(layer)
    is_interpolated = source_date != display_date
    visual_empty = bool(base_metadata.get("visual_empty"))
    empty_reason = base_metadata.get("empty_reason")
    visual_state = str(base_metadata.get("visual_state") or ("empty" if visual_empty else ("interpolated" if is_interpolated else "ready")))
    if visual_state == "warming":
        visual_state = "warming"
    elif visual_empty:
        visual_state = "empty"
    else:
        visual_state = "interpolated" if is_interpolated else "ready"
    selection_reason = str(base_metadata.get("selection_reason") or ("runtime_bucket_carry_forward" if is_interpolated else "runtime_bucket_probe"))
    fusion_mode = base_metadata.get("fusion_mode")
    if public_id == "alerta" and not fusion_mode and visual_state not in {"empty", "missing", "warming"}:
        fusion_mode = "s1_s2_carry_forward" if is_interpolated else "s1_s2"
    effective_cache_status = str(base_metadata.get("cache_status") or ("empty" if visual_empty else "ready"))
    return {
        **base_metadata,
        "layer_id": public_id,
        "display_date": display_date.isoformat(),
        "available": visual_state in {"ready", "interpolated"},
        "primary_source_date": source_date.isoformat(),
        "secondary_source_date": None,
        "resolved_source_date": source_date.isoformat(),
        "is_interpolated": is_interpolated,
        "label": "Interpolado" if is_interpolated else "Real",
        "selection_reason": selection_reason,
        "source_locked": True,
        "visual_empty": visual_empty,
        "visual_state": visual_state,
        "skip_in_playback": bool(base_metadata.get("skip_in_playback")) or visual_state in {"warming", "empty", "missing"},
        "empty_reason": empty_reason,
        "fusion_mode": fusion_mode,
        "cache_status": effective_cache_status,
        "warm_available": effective_cache_status == "ready" and visual_state in {"ready", "interpolated"} and not visual_empty,
        "frame_signature": frame_signature or _build_frame_signature(
            layer_id=public_id,
            display_date=display_date.isoformat(),
            resolved_source_date=source_date.isoformat(),
            visual_state=visual_state,
            selection_reason=selection_reason,
            source_locked=True,
            cache_status=effective_cache_status,
            empty_reason=str(empty_reason) if empty_reason else None,
            fusion_mode=str(fusion_mode) if fusion_mode else None,
        ),
    }


async def fetch_tile_png(
    layer: str,
    z: int,
    x: int,
    y: int,
    *,
    target_date: date | None = None,
    requested_source_date: date | None = None,
    frame_role: str | None = None,
    frame_signature: str | None = None,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    viewport_bbox: str | None = None,
    viewport_zoom: int | None = None,
    use_internal_products: bool = True,
) -> bytes:
    resolved_layer = resolve_temporal_layer_id(layer)
    if resolved_layer not in EVALSCRIPTS or z < TILE_MIN_ZOOM or z > TILE_MAX_ZOOM:
        return TRANSPARENT_PNG

    effective_date = _effective_source_date(target_date)
    bbox = tile_to_bbox(z, x, y)
    viewport_bbox_values = parse_bbox_values(viewport_bbox)
    resolved_zoom = _normalized_temporal_zoom(viewport_zoom, tile_zoom=z)
    bbox_bucket = viewport_bucket(
        viewport_bbox_values if viewport_bbox_values is not None else bbox,
        zoom=resolved_zoom,
    )
    requested_detail_zoom = max(int(z), int(resolved_zoom))
    canonical_internal_zoom = _canonical_internal_zoom_for_layer(resolved_layer)
    normalized_scope_ref = _normalized_scope_ref(
        scope_type=scope_type,
        scope_ref=scope_ref,
        scope=scope,
        unit_id=unit_id,
        department=department,
    )
    normalized_scope_kind = str(scope_type or scope or "nacional").strip().lower() or "nacional"
    allow_canonical_internal_products = bool(use_internal_products and _serve_tiles_internal_enabled())
    internal_only_cutover = _internal_only_cutover_active(resolved_layer, normalized_scope_kind)
    if allow_canonical_internal_products:
        canonical_tile = None
        canonical_metadata = None
        if getattr(settings, "tileserver_enabled", False):
            try:
                canonical_tile, canonical_metadata = await fetch_tileserver_tile(
                    layer_id=resolved_layer,
                    display_date=effective_date,
                    z=z,
                    x=x,
                    y=y,
                    unit_id=unit_id,
                    department=department,
                    scope_type=normalized_scope_kind,
                    scope_ref=normalized_scope_ref,
                )
            except Exception:
                canonical_tile, canonical_metadata = None, None
        if canonical_metadata is None:
            canonical_tile, canonical_metadata = await render_canonical_raster_tile(
                layer_id=resolved_layer,
                display_date=effective_date,
                x=x,
                y=y,
                z=z,
                unit_id=unit_id,
                department=department,
                scope_type=normalized_scope_kind,
                scope_ref=normalized_scope_ref,
            )
        if canonical_metadata is not None:
            canonical_tile_visually_empty = (
                canonical_tile is None
                or bool(canonical_metadata.get("visual_empty"))
                or _tile_content_is_visually_empty(canonical_tile, layer=resolved_layer)
            )
            canonical_tile_usable = bool(canonical_tile) and not canonical_tile_visually_empty and (
                resolved_layer != "rgb" or _tile_content_is_good_enough(canonical_tile, layer=resolved_layer)
            )
            if canonical_tile_usable:
                await _record_temporal_tile_cache_result(
                    layer=resolved_layer,
                    display_date=effective_date,
                    source_date=canonical_metadata.get("resolved_source_date") or effective_date.isoformat(),
                    zoom=resolved_zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    content=canonical_tile,
                    frame_metadata={
                        **canonical_metadata,
                        "coverage_origin": str(canonical_metadata.get("coverage_origin") or "internal_product"),
                        "resolved_from_cache": True,
                    },
                    status="ready",
                )
                return canonical_tile
            if canonical_tile_visually_empty and (
                canonical_metadata.get("visual_empty") or canonical_metadata.get("visual_state") in {"empty", "missing"}
            ):
                await _record_temporal_tile_cache_result(
                    layer=resolved_layer,
                    display_date=effective_date,
                    source_date=canonical_metadata.get("resolved_source_date") or effective_date.isoformat(),
                    zoom=resolved_zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    content=TRANSPARENT_PNG,
                    frame_metadata={
                        **canonical_metadata,
                        "coverage_origin": str(canonical_metadata.get("coverage_origin") or "internal_product"),
                        "resolved_from_cache": True,
                    },
                    status="empty",
                )
                return TRANSPARENT_PNG
    if allow_canonical_internal_products:
        product_tile, product_metadata = await read_viewport_raster_product_tile(
            layer_id=resolved_layer,
            display_date=effective_date,
            zoom=resolved_zoom,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=normalized_scope_ref,
            x=x,
            y=y,
        )
        if product_metadata is None:
            product_tile, product_metadata = await read_scope_viewport_raster_fallback_tile(
                layer_id=resolved_layer,
                display_date=effective_date,
                scope_type=normalized_scope_kind,
                scope_ref=normalized_scope_ref,
                x=x,
                y=y,
                z=z,
            )
        if product_metadata is not None:
            product_tile_visually_empty = (
                product_tile is None
                or bool(product_metadata.get("visual_empty"))
                or _tile_content_is_visually_empty(product_tile, layer=resolved_layer)
            )
            product_tile_usable = bool(product_tile) and not product_tile_visually_empty and (
                resolved_layer != "rgb" or _tile_content_is_good_enough(product_tile, layer=resolved_layer)
            )
            if product_tile_usable:
                await _record_temporal_tile_cache_result(
                    layer=resolved_layer,
                    display_date=effective_date,
                    source_date=product_metadata.get("source_date") or effective_date.isoformat(),
                    zoom=resolved_zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    content=product_tile,
                    frame_metadata={
                        **product_metadata,
                        "coverage_origin": str(product_metadata.get("coverage_origin") or "viewport_bucket_product"),
                        "resolved_from_cache": True,
                    },
                    status="ready",
                )
                return product_tile
            if product_tile_visually_empty:
                await _record_temporal_tile_cache_result(
                    layer=resolved_layer,
                    display_date=effective_date,
                    source_date=product_metadata.get("source_date") or effective_date.isoformat(),
                    zoom=resolved_zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    content=TRANSPARENT_PNG,
                    frame_metadata={
                        **product_metadata,
                        "coverage_origin": str(product_metadata.get("coverage_origin") or "viewport_bucket_product"),
                        "resolved_from_cache": True,
                        "visual_empty": True,
                        "visual_state": "empty",
                    },
                    status="empty",
                )
                return TRANSPARENT_PNG
        if internal_only_cutover:
            await _record_temporal_tile_cache_result(
                layer=resolved_layer,
                display_date=effective_date,
                source_date=effective_date.isoformat(),
                zoom=resolved_zoom,
                bbox_bucket=bbox_bucket,
                scope_type=scope_type,
                scope_ref=normalized_scope_ref,
                content=TRANSPARENT_PNG,
                frame_metadata={
                    "layer_id": _public_layer_id(resolved_layer),
                    "display_date": effective_date.isoformat(),
                    "resolved_source_date": None,
                    "visual_state": "missing",
                    "visual_empty": True,
                    "selection_reason": "internal_only_cutover_miss",
                    "coverage_origin": "internal_only_cutover",
                    "cache_status": "missing",
                    "skip_in_playback": True,
                    "source_locked": True,
                },
                status="missing",
            )
            return TRANSPARENT_PNG
    source_metadata = await _resolve_timeline_source_metadata(
        resolved_layer,
        effective_date,
        bbox_bucket=bbox_bucket,
        bbox=viewport_bbox,
        zoom=resolved_zoom,
        scope=scope,
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    locked_source_date = _effective_source_date(requested_source_date) if requested_source_date is not None else None
    if locked_source_date is not None:
        source_metadata = _lock_frame_metadata_to_source_date(
            layer=resolved_layer,
            display_date=effective_date,
            source_date=locked_source_date,
            base_metadata=source_metadata,
            frame_signature=frame_signature,
        )
    elif frame_signature and "frame_signature" not in source_metadata:
        source_metadata = {
            **source_metadata,
            "frame_signature": frame_signature,
        }
    if internal_only_cutover:
        await _record_temporal_tile_cache_result(
            layer=resolved_layer,
            display_date=effective_date,
            source_date=source_metadata.get("resolved_source_date") or source_metadata.get("primary_source_date") or effective_date.isoformat(),
            zoom=resolved_zoom,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=normalized_scope_ref,
            content=TRANSPARENT_PNG,
            frame_metadata={
                **source_metadata,
                "visual_state": str(source_metadata.get("visual_state") or "missing"),
                "visual_empty": bool(source_metadata.get("visual_empty", True)),
                "selection_reason": "internal_only_cutover",
                "coverage_origin": "internal_only_cutover",
                "skip_in_playback": bool(source_metadata.get("skip_in_playback", True)),
            },
            status="empty" if _source_metadata_is_known_empty(source_metadata) else "missing",
        )
        return TRANSPARENT_PNG
    if _disable_heuristic_ready_enabled() and str(source_metadata.get("selection_reason") or "") == "heuristic_fallback":
        await _record_temporal_tile_cache_result(
            layer=resolved_layer,
            display_date=effective_date,
            source_date=effective_date.isoformat(),
            zoom=resolved_zoom,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=normalized_scope_ref,
            content=TRANSPARENT_PNG,
            frame_metadata={
                **source_metadata,
                "visual_state": "missing",
                "visual_empty": True,
                "selection_reason": "heuristic_ready_disabled",
                "coverage_origin": "heuristic_ready_disabled",
                "skip_in_playback": True,
                "empty_reason": str(source_metadata.get("empty_reason") or "heuristic_ready_disabled"),
            },
            status="missing",
        )
        return TRANSPARENT_PNG
    if _source_metadata_is_known_empty(source_metadata):
        await _record_temporal_tile_cache_result(
            layer=resolved_layer,
            display_date=effective_date,
            source_date=source_metadata.get("primary_source_date") or effective_date.isoformat(),
            zoom=resolved_zoom,
            bbox_bucket=bbox_bucket,
            scope_type=scope_type,
            scope_ref=normalized_scope_ref,
            content=TRANSPARENT_PNG,
            frame_metadata=source_metadata,
            status="empty",
        )
        return TRANSPARENT_PNG
    if legacy_get_token is None or not settings.copernicus_enabled:
        return TRANSPARENT_PNG

    best_content: bytes | None = None
    best_source_date: date | None = None
    token: str | None = None
    allow_runtime_unlock = locked_source_date is None and not frame_signature
    try:
        token = await asyncio.to_thread(legacy_get_token)
        request_attempts = _temporal_tile_request_attempts(
            layer=resolved_layer,
            effective_date=effective_date,
            frame_role=frame_role,
            source_metadata=source_metadata,
        )
        best_visible_pct = -1.0
        source_locked = _metadata_bool(source_metadata, "source_locked") or str(source_metadata.get("selection_reason") or "") != "heuristic_fallback"
        def _remember_best(content: bytes, source_date: date) -> None:
            nonlocal best_content, best_visible_pct, best_source_date
            visible_pct = _png_visible_pixel_pct(content)
            score = -1.0 if visible_pct is None else visible_pct
            if score > best_visible_pct:
                best_visible_pct = score
                best_content = content
                best_source_date = source_date

        for source_date, widen_window in request_attempts:
            content = await _fetch_temporal_tile_attempt(
                layer=resolved_layer,
                z=z,
                x=x,
                y=y,
                display_date=effective_date,
                source_date=source_date,
                widen_window=widen_window,
                token=token,
                frame_role=frame_role,
                source_metadata=source_metadata,
                scope=scope,
                unit_id=unit_id,
                department=department,
                scope_type=scope_type,
                scope_ref=normalized_scope_ref,
                bbox_bucket=bbox_bucket,
            )
            if not content:
                continue
            if _tile_content_is_good_enough(content, layer=resolved_layer):
                await _record_temporal_tile_cache_result(
                    layer=resolved_layer,
                    display_date=effective_date,
                    source_date=source_date,
                    zoom=resolved_zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    content=content,
                    frame_metadata={**source_metadata, "resolved_source_date": source_date.isoformat()},
                    status="ready",
                )
                return content
            _remember_best(content, source_date)
    except Exception:
        pass

    if best_content is not None:
        if resolved_layer == "alerta_fusion" and source_locked and not token:
            await _record_temporal_tile_cache_result(
                layer=resolved_layer,
                display_date=effective_date,
                source_date=source_metadata.get("primary_source_date") or effective_date.isoformat(),
                zoom=resolved_zoom,
                bbox_bucket=bbox_bucket,
                scope_type=scope_type,
                scope_ref=normalized_scope_ref,
                content=TRANSPARENT_PNG,
                frame_metadata={
                    **source_metadata,
                    "coverage_origin": str(source_metadata.get("coverage_origin") or "source_locked_partial_tile"),
                    "visual_state": str(source_metadata.get("visual_state") or "empty"),
                    "visual_empty": False,
                },
                status="empty",
            )
            return TRANSPARENT_PNG
        if not (CAPAS_INFO.get(resolved_layer, {}).get("clouds") and source_locked):
            await _record_temporal_tile_cache_result(
                layer=resolved_layer,
                display_date=effective_date,
                source_date=best_source_date or source_metadata.get("primary_source_date") or effective_date.isoformat(),
                zoom=resolved_zoom,
                bbox_bucket=bbox_bucket,
                scope_type=scope_type,
                scope_ref=normalized_scope_ref,
                content=best_content,
                frame_metadata={
                    **source_metadata,
                    "coverage_origin": str(
                        source_metadata.get("coverage_origin")
                        or ("source_locked_partial_tile" if source_locked and CAPAS_INFO.get(resolved_layer, {}).get("clouds") else "runtime_remote_fallback")
                    ),
                    "resolved_source_date": (best_source_date or source_metadata.get("primary_source_date") or effective_date.isoformat()),
                    "visual_empty": False,
                    "visual_state": str(source_metadata.get("visual_state") or "ready"),
                },
                status="ready",
            )
            return best_content

    if allow_runtime_unlock and CAPAS_INFO.get(resolved_layer, {}).get("clouds") and source_locked and token:
        unlocked_metadata = {
            **source_metadata,
            "selection_reason": "heuristic_fallback",
            "source_locked": False,
            "resolved_source_date": None,
        }
        retry_best_content: bytes | None = best_content
        retry_best_source_date: date | None = best_source_date
        retry_best_visible_pct = best_visible_pct
        try:
            for source_date, widen_window in _temporal_tile_request_attempts(
                layer=resolved_layer,
                effective_date=effective_date,
                frame_role=frame_role,
                source_metadata=unlocked_metadata,
            ):
                content = await _fetch_temporal_tile_attempt(
                    layer=resolved_layer,
                    z=z,
                    x=x,
                    y=y,
                    display_date=effective_date,
                    source_date=source_date,
                    widen_window=widen_window,
                    token=token,
                    frame_role=frame_role,
                    source_metadata=unlocked_metadata,
                    scope=scope,
                    unit_id=unit_id,
                    department=department,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    bbox_bucket=bbox_bucket,
                )
                if not content:
                    continue
                visible_pct = _png_visible_pixel_pct(content)
                score = -1.0 if visible_pct is None else visible_pct
                if score > retry_best_visible_pct:
                    retry_best_visible_pct = score
                    retry_best_content = content
                    retry_best_source_date = source_date
                if _tile_content_is_good_enough(content, layer=resolved_layer):
                    fallback_frame_metadata = _runtime_tile_unlock_metadata(
                        layer=resolved_layer,
                        display_date=effective_date,
                        source_date=source_date,
                        base_metadata=source_metadata,
                    )
                    await _record_temporal_tile_cache_result(
                        layer=resolved_layer,
                        display_date=effective_date,
                        source_date=source_date,
                        zoom=resolved_zoom,
                        bbox_bucket=bbox_bucket,
                        scope_type=scope_type,
                        scope_ref=normalized_scope_ref,
                        content=content,
                        frame_metadata=fallback_frame_metadata,
                        status="ready",
                    )
                    await _persist_timeline_source_metadata(
                        layer=resolved_layer,
                        display_date=effective_date,
                        bbox_bucket=bbox_bucket,
                        scope_type=scope_type,
                        scope_ref=normalized_scope_ref,
                        metadata=fallback_frame_metadata,
                    )
                    return content
        except Exception:
            pass
        if retry_best_content is not None:
            retry_source_date = retry_best_source_date or effective_date
            fallback_frame_metadata = _runtime_tile_unlock_metadata(
                layer=resolved_layer,
                display_date=effective_date,
                source_date=retry_source_date,
                base_metadata=source_metadata,
            )
            if resolved_layer == "rgb" and not _tile_content_is_good_enough(retry_best_content, layer=resolved_layer):
                retry_best_content = None
            else:
                await _record_temporal_tile_cache_result(
                    layer=resolved_layer,
                    display_date=effective_date,
                    source_date=retry_source_date,
                    zoom=resolved_zoom,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    content=retry_best_content,
                    frame_metadata=fallback_frame_metadata,
                    status="ready",
                )
                await _persist_timeline_source_metadata(
                    layer=resolved_layer,
                    display_date=effective_date,
                    bbox_bucket=bbox_bucket,
                    scope_type=scope_type,
                    scope_ref=normalized_scope_ref,
                    metadata=fallback_frame_metadata,
                )
                return retry_best_content

    await _record_temporal_tile_cache_result(
        layer=resolved_layer,
        display_date=effective_date,
        source_date=source_metadata.get("primary_source_date") or effective_date.isoformat(),
        zoom=resolved_zoom,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
        content=TRANSPARENT_PNG,
        frame_metadata=source_metadata,
        status="empty",
    )
    return TRANSPARENT_PNG


def _normalize_timeline_layers(layers: list[str] | tuple[str, ...]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for layer in layers:
        internal = resolve_temporal_layer_id(layer)
        if internal and internal not in seen:
            resolved.append(internal)
            seen.add(internal)
    return resolved


def _normalize_timeline_bbox(bbox: str | None) -> str:
    if not bbox:
        return "auto"
    values: list[str] = []
    for item in str(bbox).split(",")[:4]:
        try:
            values.append(f"{float(item):.3f}")
        except Exception:
            values.append(item.strip())
    return ",".join(values) if values else "auto"


def _timeline_cache_key(
    layers: list[str],
    date_from: date,
    date_to: date,
    *,
    bbox: str | None,
    zoom: int | None,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> str:
    resolved_zoom = _normalized_temporal_zoom(zoom)
    raw = json.dumps(
        {
            "layers": layers,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "bbox": _normalize_timeline_bbox(bbox),
            "zoom": resolved_zoom,
            "window_days": TIMELINE_FRAME_WINDOW_DAYS,
            "scope": scope,
            "unit_id": unit_id,
            "department": department,
            "scope_type": scope_type,
            "scope_ref": scope_ref,
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _timeline_zoom_levels(zoom: int | None) -> list[int]:
    base_zoom = _normalized_temporal_zoom(zoom)
    adjacent_zoom = min(TILE_MAX_ZOOM, base_zoom + max(int(settings.preload_adjacent_zoom_delta), 0))
    zooms = [base_zoom]
    if adjacent_zoom not in zooms:
        zooms.append(adjacent_zoom)
    return zooms


async def _timeline_frame_cache_status_index(
    *,
    layers: list[str],
    date_from: date,
    date_to: date,
    bbox: str | None,
    zoom: int | None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, dict[str, str]]:
    if not layers:
        return {}
    zoom_levels = _timeline_zoom_levels(zoom)
    bbox_buckets = [viewport_bucket(bbox, zoom=item) for item in zoom_levels]
    async with AsyncSessionLocal() as session:
        return await get_raster_cache_status_index(
            session,
            layer_ids=[TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in layers],
            cache_kind="analytic_tile",
            date_from=date_from,
            date_to=date_to,
            bbox_bucket=bbox_buckets,
            zoom_levels=zoom_levels,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )


def _iter_dates(date_from: date, date_to: date):
    current = date_from
    while current <= date_to:
        yield current
        current += timedelta(days=1)


def _anchor_frame_dates(layer: str, display_date: date) -> tuple[date, date | None, float, bool]:
    config = TEMPORAL_LAYER_CONFIGS[layer]
    revisit_days = int(config.get("revisit_days", 1))
    if revisit_days <= 1:
        return display_date, None, 0.0, False

    anchor_date = config.get("anchor_date") or date(2020, 1, 1)
    delta_days = (display_date - anchor_date).days
    previous_index = delta_days // revisit_days
    previous_anchor = anchor_date + timedelta(days=previous_index * revisit_days)
    if previous_anchor > display_date:
        previous_anchor -= timedelta(days=revisit_days)
    next_anchor = previous_anchor + timedelta(days=revisit_days)
    if display_date == previous_anchor:
        return previous_anchor, None, 0.0, False
    if next_anchor > date.today():
        return previous_anchor, None, 0.0, True
    blend_weight = round((display_date - previous_anchor).days / revisit_days, 3)
    return previous_anchor, next_anchor, blend_weight, True


def _timeline_day_label(display_date: date, layer_frames: dict[str, dict[str, Any]]) -> str:
    if not layer_frames:
        return display_date.isoformat()
    if all(not frame.get("is_interpolated") for frame in layer_frames.values()):
        return f"{display_date.isoformat()} · Real"
    return f"{display_date.isoformat()} · Interpolado"


async def build_timeline_frame_manifest(
    *,
    layers: list[str] | tuple[str, ...],
    date_from: date | None = None,
    date_to: date | None = None,
    bbox: str | None = None,
    zoom: int | None = None,
    scope: str | None = None,
    unit_id: str | None = None,
    department: str | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
) -> dict[str, Any]:
    resolved_layers = _normalize_timeline_layers(list(layers))
    resolved_zoom = _normalized_temporal_zoom(zoom)
    normalized_scope_kind = str(scope_type or scope or "nacional").strip().lower()
    today = date.today()
    resolved_date_to = min(date_to or today, today)
    resolved_date_from = date_from or (resolved_date_to - timedelta(days=TIMELINE_FRAME_WINDOW_DAYS - 1))
    if resolved_date_from > resolved_date_to:
        resolved_date_from = resolved_date_to

    cache_key = _timeline_cache_key(
        resolved_layers,
        resolved_date_from,
        resolved_date_to,
        bbox=bbox,
        zoom=resolved_zoom,
        scope=scope,
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=scope_ref,
    )
    cached = TIMELINE_MANIFEST_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < TIMELINE_MANIFEST_CACHE_TTL_SECONDS:
        return cached[1]

    cache_status_index = await _timeline_frame_cache_status_index(
        layers=resolved_layers,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        bbox=bbox,
        zoom=resolved_zoom,
        scope_type=scope_type,
        scope_ref=_normalized_scope_ref(
            scope_type=scope_type,
            scope_ref=scope_ref,
            scope=scope,
            unit_id=unit_id,
            department=department,
        ),
    )
    bbox_bucket = viewport_bucket(bbox, zoom=resolved_zoom)
    normalized_scope_ref = _normalized_scope_ref(
        scope_type=scope_type,
        scope_ref=scope_ref,
        scope=scope,
        unit_id=unit_id,
        department=department,
    )
    persisted_source_index = await _load_persisted_timeline_source_metadata_index(
        layers=resolved_layers,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        bbox_bucket=bbox_bucket,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    canonical_product_index = await get_canonical_product_status_index(
        layer_ids=resolved_layers,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        unit_id=unit_id,
        department=department,
        scope_type=scope_type,
        scope_ref=normalized_scope_ref,
    )
    days: list[dict[str, Any]] = []
    for display_date in _iter_dates(resolved_date_from, resolved_date_to):
        layer_frames: dict[str, dict[str, Any]] = {}
        for internal_layer in resolved_layers:
            product_metadata = canonical_product_index.get(internal_layer, {}).get(display_date.isoformat())
            if product_metadata is not None:
                frame_metadata = _frame_metadata_from_canonical_product(
                    layer=internal_layer,
                    display_date=display_date,
                    metadata=product_metadata,
                )
            else:
                persisted_metadata = persisted_source_index.get(_public_layer_id(internal_layer), {}).get(display_date.isoformat())
                if persisted_metadata is not None:
                    frame_metadata = persisted_metadata
                elif (
                    not _disable_heuristic_ready_enabled()
                    and display_date == resolved_date_to
                    and bbox
                    and zoom is not None
                ):
                    frame_metadata = await _resolve_timeline_source_metadata(
                        internal_layer,
                        display_date,
                        bbox_bucket=bbox_bucket,
                        bbox=bbox,
                        zoom=resolved_zoom,
                        scope=scope,
                        unit_id=unit_id,
                        department=department,
                        scope_type=scope_type,
                        scope_ref=normalized_scope_ref,
                        allow_runtime_probe=True,
                    )
                else:
                    frame_metadata = _missing_frame_metadata_from_heuristic(
                        layer=internal_layer,
                        display_date=display_date,
                        fallback_metadata={
                            "primary_source_date": display_date.isoformat(),
                            "secondary_source_date": None,
                            "blend_weight": 0.0,
                            "selection_reason": "missing_product_metadata",
                            "renderable_pixel_pct": 0.0,
                            "visual_empty": True,
                            "empty_reason": "missing_product_metadata",
                            "resolved_source_date": None,
                            "source_locked": True,
                        },
                        reason="missing_product_metadata",
                    )
            cache_status = cache_status_index.get(frame_metadata["layer_id"], {}).get(display_date.isoformat(), "missing")
            if frame_metadata.get("cache_status") and cache_status == "missing":
                cache_status = str(frame_metadata.get("cache_status"))
            visual_state = frame_metadata.get("visual_state") or "missing"
            visual_empty = bool(frame_metadata.get("visual_empty"))
            empty_reason = frame_metadata.get("empty_reason")
            if cache_status == "empty":
                previous_visual_state = str(frame_metadata.get("visual_state") or "")
                previous_visual_empty = bool(frame_metadata.get("visual_empty"))
                visual_state = "empty"
                visual_empty = True
                if not previous_visual_empty and previous_visual_state not in {"empty", "missing"}:
                    empty_reason = "warm_cache_empty"
            if visual_state not in {"empty", "missing"} and cache_status == "warming":
                visual_state = "warming"
            available = visual_state in {"ready", "interpolated"}
            resolved_source_date = frame_metadata.get("resolved_source_date") or frame_metadata.get("primary_source_date")
            source_locked = _metadata_bool(frame_metadata, "source_locked", True)
            selection_reason = str(frame_metadata.get("selection_reason") or frame_metadata.get("availability") or "missing")
            fusion_mode = frame_metadata.get("fusion_mode")
            frame_signature = str(frame_metadata.get("frame_signature") or _build_frame_signature(
                layer_id=frame_metadata["layer_id"],
                display_date=display_date.isoformat(),
                resolved_source_date=str(resolved_source_date) if resolved_source_date else None,
                visual_state=visual_state,
                selection_reason=selection_reason,
                source_locked=source_locked,
                cache_status=cache_status,
                empty_reason=str(empty_reason) if empty_reason else None,
                fusion_mode=str(fusion_mode) if fusion_mode else None,
            ))
            layer_frames[frame_metadata["layer_id"]] = {
                **frame_metadata,
                "available": available,
                "visual_empty": visual_empty,
                "visual_state": visual_state,
                "skip_in_playback": bool(frame_metadata.get("skip_in_playback")) or visual_state in {"warming", "empty", "missing"},
                "empty_reason": empty_reason,
                "cache_status": cache_status,
                "warm_available": cache_status == "ready" and available and not visual_empty,
                "resolved_source_date": str(resolved_source_date) if resolved_source_date else None,
                "source_locked": source_locked,
                "frame_signature": frame_signature,
            }
        days.append(
            {
                "display_date": display_date.isoformat(),
                "available": any(frame.get("available", False) for frame in layer_frames.values()) if layer_frames else False,
                "label": _timeline_day_label(display_date, layer_frames),
                "layers": layer_frames,
            }
        )

    payload = {
        "date_from": resolved_date_from.isoformat(),
        "date_to": resolved_date_to.isoformat(),
        "total_days": len(days),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bbox": bbox,
        "zoom": zoom,
        "scope": scope,
        "unit_id": unit_id,
        "department": department,
        "scope_type": scope_type,
        "scope_ref": normalized_scope_ref,
        "layers": [TEMPORAL_LAYER_CONFIGS[layer]["public_id"] for layer in resolved_layers],
        "days": days,
    }
    TIMELINE_MANIFEST_CACHE[cache_key] = (time.time(), payload)
    return payload


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
    encoded = urlencode([("_source", CONEAT_CACHE_NAMESPACE), *sorted(normalized.items())], doseq=True)
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


def _coneat_arcgis_format(format_value: str) -> str:
    normalized = str(format_value or "image/png").strip().lower()
    if normalized in {"image/png32", "png32"}:
        return "png32"
    if normalized in {"image/png24", "png24"}:
        return "png24"
    if normalized in {"image/png", "png"}:
        return "png"
    if normalized in {"image/jpeg", "image/jpg", "jpg", "jpeg"}:
        return "jpg"
    return "png"


def _coneat_srid_from_request(params: dict) -> str:
    raw = str(params.get("CRS") or params.get("SRS") or "EPSG:4326").strip().upper()
    if raw.startswith("EPSG:"):
        return raw.split(":", 1)[1]
    return "4326"


def _coneat_export_params(params: dict) -> dict[str, str]:
    width = str(params.get("WIDTH") or "512")
    height = str(params.get("HEIGHT") or "512")
    srid = _coneat_srid_from_request(params)
    export_params = {
        "f": "image",
        "bbox": str(params.get("BBOX") or ""),
        "bboxSR": srid,
        "imageSR": srid,
        "size": f"{width},{height}",
        "format": _coneat_arcgis_format(str(params.get("FORMAT") or "image/png")),
        "transparent": str(params.get("TRANSPARENT") or "true").lower(),
        # Let SNIG decide which scale-dependent layer to draw. The old WMS layer ids are obsolete.
        "layers": "show:0,1",
    }
    return export_params


async def _fetch_coneat_remote(params: dict) -> tuple[bytes, str]:
    request_name = _coneat_request_name(params)
    headers = {
        "User-Agent": "AgroClimaX/1.0 CONEAT proxy",
        "Accept": _coneat_default_content_type(params),
    }
    timeout = httpx.Timeout(connect=8.0, read=30.0, write=15.0, pool=15.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        if request_name == "GETMAP":
            response = await client.get(CONEAT_EXPORT_URL, params=_coneat_export_params(params), headers=headers)
        elif request_name == "GETCAPABILITIES":
            response = await client.get(CONEAT_INFO_URL, params={"f": "pjson"}, headers=headers)
        else:
            response = await client.get(CONEAT_INFO_URL, params={"f": "pjson"}, headers=headers)
        response.raise_for_status()
    return response.content, response.headers.get("content-type", _coneat_default_content_type(params))


async def proxy_coneat_request(params: dict) -> tuple[bytes, str]:
    request_name = _coneat_request_name(params)
    if request_name == "GETMAP":
        normalized = _normalize_coneat_params(params)
        return await proxy_official_overlay_tile(
            "coneat",
            {
                "bbox": normalized.get("BBOX", ""),
                "bboxSR": _coneat_srid_from_request(normalized),
                "imageSR": _coneat_srid_from_request(normalized),
                "width": normalized.get("WIDTH", "512"),
                "height": normalized.get("HEIGHT", "512"),
                "format": normalized.get("FORMAT", "image/png"),
                "transparent": normalized.get("TRANSPARENT", "true"),
            },
        )
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
                content, content_type = await _fetch_coneat_remote(normalized_params)
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


def list_official_map_overlays() -> list[dict[str, object]]:
    overlays = []
    for overlay in sorted(OFFICIAL_MAP_OVERLAYS.values(), key=lambda item: int(item.get("z_index_priority") or 0)):
        overlays.append(
            {
                "id": overlay["id"],
                "label": overlay["label"],
                "category": overlay["category"],
                "provider": overlay["provider"],
                "service_kind": overlay["service_kind"],
                "service_url": overlay["service_url"],
                "layers": overlay["layers"],
                "min_zoom": overlay["min_zoom"],
                "opacity_default": overlay["opacity_default"],
                "z_index_priority": overlay["z_index_priority"],
                "attribution": overlay["attribution"],
                "cache_namespace": overlay["cache_namespace"],
                "recommended": bool(overlay.get("recommended")),
            }
        )
    return overlays


def _official_overlay_bucket_object_key(overlay_id: str, cache_key: str) -> str:
    return f"external-map-cache/official-overlays/{overlay_id}/{cache_key}.bin"


def _official_overlay_definition(overlay_id: str) -> dict[str, object]:
    overlay = OFFICIAL_MAP_OVERLAYS.get(overlay_id)
    if overlay is None:
        raise KeyError(overlay_id)
    return overlay


def _normalize_official_overlay_params(params: dict[str, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in params.items():
        key_str = str(key)
        value_str = str(value).strip()
        lower_key = key_str.lower()
        if lower_key == "bbox":
            try:
                value_str = ",".join(f"{float(part):.6f}" for part in value_str.split(",")[:4])
            except Exception:
                pass
        elif lower_key in {"width", "height"}:
            try:
                value_str = str(int(float(value_str)))
            except Exception:
                pass
        normalized[lower_key] = value_str
    normalized.setdefault("bboxsr", "4326")
    normalized.setdefault("imagesr", normalized["bboxsr"])
    normalized.setdefault("width", "256")
    normalized.setdefault("height", "256")
    normalized.setdefault("format", "image/png")
    normalized.setdefault("transparent", "true")
    return normalized


def _official_overlay_cache_key(overlay_id: str, overlay: dict[str, object], params: dict[str, str]) -> str:
    encoded = urlencode(
        [
            ("_overlay", overlay_id),
            ("_namespace", str(overlay.get("cache_namespace") or "")),
            *sorted(params.items()),
        ],
        doseq=True,
    )
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _official_overlay_cache_paths(overlay_id: str, cache_key: str) -> tuple[Path, Path]:
    overlay_dir = OFFICIAL_OVERLAY_CACHE_DIR / overlay_id
    overlay_dir.mkdir(exist_ok=True)
    return overlay_dir / f"{cache_key}.bin", overlay_dir / f"{cache_key}.json"


def _read_official_overlay_cache(cache_path: Path, meta_path: Path, default_content_type: str) -> tuple[bytes, str] | None:
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


def _write_official_overlay_cache(cache_path: Path, meta_path: Path, content: bytes, content_type: str) -> None:
    cache_path.write_bytes(content)
    meta_path.write_text(json.dumps({"content_type": content_type}), encoding="utf-8")


async def _read_official_overlay_cache_db(
    overlay_id: str,
    cache_key: str,
    default_content_type: str,
) -> tuple[bytes, str] | None:
    provider = f"overlay:{overlay_id}"
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ExternalMapCacheEntry).where(
                ExternalMapCacheEntry.provider == provider,
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


async def _write_official_overlay_cache_db(
    overlay_id: str,
    cache_key: str,
    params: dict[str, str],
    content: bytes,
    content_type: str,
) -> None:
    provider = f"overlay:{overlay_id}"
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ExternalMapCacheEntry).where(
                ExternalMapCacheEntry.provider == provider,
                ExternalMapCacheEntry.cache_key == cache_key,
            ).limit(1)
        )
        row = result.scalar_one_or_none()
        if row is None:
            row = ExternalMapCacheEntry(
                cache_key=cache_key,
                provider=provider,
                request_name="EXPORT",
                content_type=content_type,
                content=content,
                content_hash=hashlib.sha1(content).hexdigest(),
                expires_at=datetime.utcnow() + timedelta(hours=settings.coneat_cache_ttl_hours),
                metadata_extra={"params": params, "overlay_id": overlay_id},
            )
            session.add(row)
        else:
            row.request_name = "EXPORT"
            row.content_type = content_type
            row.content = content
            row.content_hash = hashlib.sha1(content).hexdigest()
            row.expires_at = datetime.utcnow() + timedelta(hours=settings.coneat_cache_ttl_hours)
            row.metadata_extra = {"params": params, "overlay_id": overlay_id}
        await session.commit()


def _official_overlay_arcgis_export_params(overlay: dict[str, object], params: dict[str, str]) -> dict[str, str]:
    export_params = {
        "f": "image",
        "bbox": params["bbox"],
        "bboxSR": params["bboxsr"],
        "imageSR": params["imagesr"],
        "size": f"{params['width']},{params['height']}",
        "format": _coneat_arcgis_format(params.get("format", "image/png")),
        "transparent": params.get("transparent", "true").lower(),
    }
    overlay_layers = str(overlay.get("layers") or "").strip()
    if overlay_layers:
        export_params["layers"] = overlay_layers
    return export_params


async def _fetch_official_overlay_remote(overlay: dict[str, object], params: dict[str, str]) -> tuple[bytes, str]:
    headers = {
        "User-Agent": "AgroClimaX/1.0 official overlay proxy",
        "Accept": "image/png",
    }
    timeout = httpx.Timeout(connect=8.0, read=30.0, write=15.0, pool=15.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(
            str(overlay["service_url"]),
            params=_official_overlay_arcgis_export_params(overlay, params),
            headers=headers,
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "image/png")
        if "image" not in content_type.lower():
            raise httpx.HTTPStatusError(
                "Official overlay export did not return an image",
                request=response.request,
                response=response,
            )
    return response.content, content_type


async def proxy_official_overlay_tile(overlay_id: str, params: dict[str, object]) -> tuple[bytes, str]:
    overlay = _official_overlay_definition(overlay_id)
    normalized_params = _normalize_official_overlay_params(params)
    default_content_type = "image/png"
    cache_key = _official_overlay_cache_key(overlay_id, overlay, normalized_params)
    cache_path, meta_path = _official_overlay_cache_paths(overlay_id, cache_key)

    cached = _read_official_overlay_cache(cache_path, meta_path, default_content_type)
    if cached:
        return cached

    cached = await _read_official_overlay_cache_db(overlay_id, cache_key, default_content_type)
    if cached:
        _write_official_overlay_cache(cache_path, meta_path, cached[0], cached[1])
        return cached

    bucket_cached = await storage_get_bytes(_official_overlay_bucket_object_key(overlay_id, cache_key))
    if bucket_cached:
        content, content_type, _metadata = bucket_cached
        resolved_content_type = content_type or default_content_type
        _write_official_overlay_cache(cache_path, meta_path, content, resolved_content_type)
        await _write_official_overlay_cache_db(overlay_id, cache_key, normalized_params, content, resolved_content_type)
        return content, resolved_content_type

    last_error: Exception | None = None
    async with OFFICIAL_OVERLAY_PROXY_SEMAPHORE:
        cached = _read_official_overlay_cache(cache_path, meta_path, default_content_type)
        if cached:
            return cached
        cached = await _read_official_overlay_cache_db(overlay_id, cache_key, default_content_type)
        if cached:
            _write_official_overlay_cache(cache_path, meta_path, cached[0], cached[1])
            return cached
        bucket_cached = await storage_get_bytes(_official_overlay_bucket_object_key(overlay_id, cache_key))
        if bucket_cached:
            content, content_type, _metadata = bucket_cached
            resolved_content_type = content_type or default_content_type
            _write_official_overlay_cache(cache_path, meta_path, content, resolved_content_type)
            await _write_official_overlay_cache_db(overlay_id, cache_key, normalized_params, content, resolved_content_type)
            return content, resolved_content_type
        for attempt, delay in enumerate(OFFICIAL_OVERLAY_PROXY_RETRY_DELAYS, start=1):
            try:
                content, content_type = await _fetch_official_overlay_remote(overlay, normalized_params)
                _write_official_overlay_cache(cache_path, meta_path, content, content_type)
                await _write_official_overlay_cache_db(overlay_id, cache_key, normalized_params, content, content_type)
                await storage_put_bytes(
                    _official_overlay_bucket_object_key(overlay_id, cache_key),
                    content,
                    content_type=content_type,
                )
                return content, content_type
            except (KeyError, httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt < len(OFFICIAL_OVERLAY_PROXY_RETRY_DELAYS):
                    await asyncio.sleep(delay)

    cached = _read_official_overlay_cache(cache_path, meta_path, default_content_type)
    if cached:
        return cached
    cached = await _read_official_overlay_cache_db(overlay_id, cache_key, default_content_type)
    if cached:
        _write_official_overlay_cache(cache_path, meta_path, cached[0], cached[1])
        return cached
    bucket_cached = await storage_get_bytes(_official_overlay_bucket_object_key(overlay_id, cache_key))
    if bucket_cached:
        content, content_type, _metadata = bucket_cached
        resolved_content_type = content_type or default_content_type
        _write_official_overlay_cache(cache_path, meta_path, content, resolved_content_type)
        await _write_official_overlay_cache_db(overlay_id, cache_key, normalized_params, content, resolved_content_type)
        return content, resolved_content_type

    return TRANSPARENT_PNG, "image/png"


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
