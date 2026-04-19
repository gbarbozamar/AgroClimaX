from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
from app.models.materialized import ExternalMapCacheEntry, SatelliteLayerSnapshot
from app.services.object_storage import storage_get_bytes, storage_put_bytes
from app.services.raster_cache import get_raster_cache_status_index, viewport_bucket

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

logger = logging.getLogger(__name__)

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_MIN_VALID_TILE_BYTES = 1024


def _is_valid_tile_png(data: bytes | None) -> bool:
    """PNG tile sanity check: signature + size above the transparent-fallback threshold."""
    if not data or len(data) < _MIN_VALID_TILE_BYTES:
        return False
    return data[:8] == _PNG_SIGNATURE


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
        "window_before_days": 5,
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


def _time_range_for_temporal_layer(layer: str, target_date: date) -> tuple[date, date]:
    config = TEMPORAL_LAYER_CONFIGS.get(layer)
    if not config:
        return target_date - timedelta(days=45), target_date
    before_days = int(config.get("window_before_days", 0))
    after_days = int(config.get("window_after_days", 0))
    if config.get("time_mode") == "carry_forward":
        return target_date - timedelta(days=before_days), target_date
    return target_date - timedelta(days=before_days), target_date + timedelta(days=after_days)


def _timeline_source_cache_key(layer: str, display_date: date) -> str:
    return f"{layer}::{display_date.isoformat()}"


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
) -> dict[str, dict[date, dict[str, Any]]]:
    if not layers:
        return {}
    before_padding = max((_timeline_snapshot_padding(layer)[0] for layer in layers), default=7)
    after_padding = max((_timeline_snapshot_padding(layer)[1] for layer in layers), default=7)
    start_at = datetime.combine(date_from - timedelta(days=before_padding), datetime.min.time(), tzinfo=timezone.utc)
    end_at = datetime.combine(date_to + timedelta(days=after_padding + 1), datetime.min.time(), tzinfo=timezone.utc)
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(
                    SatelliteLayerSnapshot.layer_key,
                    SatelliteLayerSnapshot.observed_at,
                    SatelliteLayerSnapshot.metadata_extra,
                    SatelliteLayerSnapshot.availability_score,
                )
                .where(
                    SatelliteLayerSnapshot.layer_key.in_(layers),
                    SatelliteLayerSnapshot.observed_at >= start_at,
                    SatelliteLayerSnapshot.observed_at < end_at,
                )
                .order_by(
                    SatelliteLayerSnapshot.layer_key,
                    SatelliteLayerSnapshot.observed_at.desc(),
                    SatelliteLayerSnapshot.availability_score.desc(),
                )
            )
            rows = result.all()
    except Exception:
        return {layer: {} for layer in layers}

    index: dict[str, dict[date, dict[str, Any]]] = {layer: {} for layer in layers}
    for layer_key, observed_at, metadata_extra, availability_score in rows:
        if observed_at is None:
            continue
        observed_date = observed_at.date()
        layer_bucket = index.setdefault(layer_key, {})
        if observed_date in layer_bucket:
            continue
        layer_bucket[observed_date] = {
            "observed_date": observed_date,
            "metadata_extra": metadata_extra or {},
            "availability_score": float(availability_score or 0.0),
        }
    return index


def _frame_metadata_from_snapshot(
    *,
    internal_layer: str,
    display_date: date,
    snapshot_index: dict[str, dict[date, dict[str, Any]]],
) -> dict[str, Any]:
    config = TEMPORAL_LAYER_CONFIGS[internal_layer]
    public_id = str(config["public_id"])
    layer_rows = snapshot_index.get(internal_layer) or {}
    exact = layer_rows.get(display_date)
    if exact is not None:
        metadata_extra = exact.get("metadata_extra") or {}
        primary_source_date = metadata_extra.get("primary_source_date") or display_date.isoformat()
        secondary_source_date = metadata_extra.get("secondary_source_date")
        blend_weight = float(metadata_extra.get("blend_weight") or 0.0)
        is_interpolated = bool(metadata_extra.get("is_interpolated")) or bool(secondary_source_date)
        label = metadata_extra.get("label") or ("Interpolado" if is_interpolated else "Real")
        availability = metadata_extra.get("availability") or ("available" if exact.get("availability_score", 0.0) > 0 else "missing")
        return {
            "layer_id": public_id,
            "available": availability != "missing",
            "availability": availability,
            "is_interpolated": is_interpolated,
            "primary_source_date": primary_source_date,
            "secondary_source_date": secondary_source_date,
            "blend_weight": blend_weight,
            "label": label,
        }

    available_dates = sorted(layer_rows.keys())
    previous_date = max((item for item in available_dates if item < display_date), default=None)
    next_date = min((item for item in available_dates if item > display_date), default=None)
    time_mode = str(config.get("time_mode") or "symmetric")

    if time_mode == "carry_forward":
        if previous_date is not None:
            previous_meta = (layer_rows[previous_date].get("metadata_extra") or {})
            return {
                "layer_id": public_id,
                "available": True,
                "availability": "historical_carry_forward",
                "is_interpolated": True,
                "primary_source_date": previous_meta.get("primary_source_date") or previous_date.isoformat(),
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Interpolado",
            }
        if next_date is not None:
            next_meta = (layer_rows[next_date].get("metadata_extra") or {})
            return {
                "layer_id": public_id,
                "available": True,
                "availability": "historical_forward_fill",
                "is_interpolated": True,
                "primary_source_date": next_meta.get("primary_source_date") or next_date.isoformat(),
                "secondary_source_date": None,
                "blend_weight": 0.0,
                "label": "Interpolado",
            }

    if previous_date is not None and next_date is not None:
        total_days = max((next_date - previous_date).days, 1)
        previous_meta = (layer_rows[previous_date].get("metadata_extra") or {})
        next_meta = (layer_rows[next_date].get("metadata_extra") or {})
        return {
            "layer_id": public_id,
            "available": True,
            "availability": "historical_blend",
            "is_interpolated": True,
            "primary_source_date": previous_meta.get("primary_source_date") or previous_date.isoformat(),
            "secondary_source_date": next_meta.get("primary_source_date") or next_date.isoformat(),
            "blend_weight": round((display_date - previous_date).days / total_days, 3),
            "label": "Interpolado",
        }
    if previous_date is not None:
        previous_meta = (layer_rows[previous_date].get("metadata_extra") or {})
        return {
            "layer_id": public_id,
            "available": True,
            "availability": "historical_previous_only",
            "is_interpolated": True,
            "primary_source_date": previous_meta.get("primary_source_date") or previous_date.isoformat(),
            "secondary_source_date": None,
            "blend_weight": 0.0,
            "label": "Interpolado",
        }
    if next_date is not None:
        next_meta = (layer_rows[next_date].get("metadata_extra") or {})
        return {
            "layer_id": public_id,
            "available": True,
            "availability": "historical_next_only",
            "is_interpolated": True,
            "primary_source_date": next_meta.get("primary_source_date") or next_date.isoformat(),
            "secondary_source_date": None,
            "blend_weight": 0.0,
            "label": "Interpolado",
        }

    primary_date, secondary_date, blend_weight, is_interpolated = _anchor_frame_dates(internal_layer, display_date)
    return {
        "layer_id": public_id,
        "available": True,
        "availability": "heuristic_fallback",
        "is_interpolated": is_interpolated,
        "primary_source_date": primary_date.isoformat(),
        "secondary_source_date": secondary_date.isoformat() if secondary_date else None,
        "blend_weight": blend_weight,
        "label": "Interpolado" if is_interpolated else "Real",
    }


async def _resolve_timeline_source_metadata(layer: str, display_date: date) -> dict[str, Any]:
    cache_key = _timeline_source_cache_key(layer, display_date)
    cached = TIMELINE_SOURCE_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < TIMELINE_MANIFEST_CACHE_TTL_SECONDS:
        return cached[1]
    snapshot_index = await _load_timeline_snapshot_index(layers=[layer], date_from=display_date, date_to=display_date)
    metadata = _frame_metadata_from_snapshot(
        internal_layer=layer,
        display_date=display_date,
        snapshot_index=snapshot_index,
    )
    TIMELINE_SOURCE_CACHE[cache_key] = (time.time(), metadata)
    return metadata


async def fetch_tile_png(
    layer: str,
    z: int,
    x: int,
    y: int,
    *,
    target_date: date | None = None,
    frame_role: str | None = None,
) -> bytes:
    resolved_layer = resolve_temporal_layer_id(layer)
    if resolved_layer not in EVALSCRIPTS or z < TILE_MIN_ZOOM or z > TILE_MAX_ZOOM:
        return TRANSPARENT_PNG

    effective_date = _effective_source_date(target_date)
    source_metadata = await _resolve_timeline_source_metadata(resolved_layer, effective_date)
    primary_source_date = source_metadata.get("primary_source_date") or effective_date.isoformat()
    secondary_source_date = source_metadata.get("secondary_source_date")
    resolved_source_date = primary_source_date
    if frame_role == "secondary" and secondary_source_date:
        resolved_source_date = secondary_source_date
    try:
        source_date = date.fromisoformat(str(resolved_source_date))
    except Exception:
        source_date = effective_date

    cache_path = TILE_CACHE_DIR / f"{resolved_layer}_{source_date.isoformat()}_{z}_{x}_{y}.png"
    if cache_path.exists():
        cached_bytes = cache_path.read_bytes()
        if _is_valid_tile_png(cached_bytes):
            return cached_bytes
        logger.warning(
            "Discarding invalid disk tile cache layer=%s z=%s x=%s y=%s size=%d",
            resolved_layer, z, x, y, len(cached_bytes),
        )
        try:
            cache_path.unlink()
        except OSError:
            pass

    tile_bucket_key = _tile_bucket_key(resolved_layer, z, x, y, target_date=source_date)
    bucket_cached = await storage_get_bytes(tile_bucket_key)
    if bucket_cached and _is_valid_tile_png(bucket_cached[0]):
        cache_path.write_bytes(bucket_cached[0])
        return bucket_cached[0]
    if bucket_cached:
        logger.warning(
            "Discarding invalid bucket tile cache layer=%s z=%s x=%s y=%s size=%d",
            resolved_layer, z, x, y, len(bucket_cached[0]),
        )

    if legacy_get_token is None or not settings.copernicus_enabled:
        return TRANSPARENT_PNG

    info = CAPAS_INFO[resolved_layer]
    bbox = tile_to_bbox(z, x, y)
    start_date, end_date = _time_range_for_temporal_layer(resolved_layer, source_date)
    data_filter = {"timeRange": {"from": f"{start_date.isoformat()}T00:00:00Z", "to": f"{end_date.isoformat()}T23:59:59Z"}}
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
        "evalscript": EVALSCRIPTS[resolved_layer],
    }
    if frame_role:
        payload["metadata"] = {
            "frame_role": frame_role,
            "display_date": effective_date.isoformat(),
            "source_date": source_date.isoformat(),
            "secondary_source_date": secondary_source_date,
            "availability": source_metadata.get("availability"),
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
            if _is_valid_tile_png(response.content):
                cache_path.write_bytes(response.content)
                await storage_put_bytes(tile_bucket_key, response.content, content_type="image/png")
                return response.content
            logger.warning(
                "Rejecting suspicious Copernicus tile layer=%s z=%s x=%s y=%s size=%d status=%s",
                resolved_layer, z, x, y, len(response.content), response.status_code,
            )
            return TRANSPARENT_PNG
    except Exception as exc:
        logger.warning(
            "fetch_tile_png exception layer=%s z=%s x=%s y=%s exc=%s",
            resolved_layer, z, x, y, type(exc).__name__,
        )
        return TRANSPARENT_PNG

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
) -> str:
    raw = json.dumps(
        {
            "layers": layers,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "bbox": _normalize_timeline_bbox(bbox),
            "zoom": int(zoom or 0),
            "window_days": TIMELINE_FRAME_WINDOW_DAYS,
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _timeline_zoom_levels(zoom: int | None) -> list[int]:
    base_zoom = int(zoom or TILE_MIN_ZOOM)
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
) -> dict[str, Any]:
    resolved_layers = _normalize_timeline_layers(list(layers))
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
        zoom=zoom,
    )
    cached = TIMELINE_MANIFEST_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < TIMELINE_MANIFEST_CACHE_TTL_SECONDS:
        return cached[1]

    snapshot_index = await _load_timeline_snapshot_index(
        layers=resolved_layers,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
    )
    cache_status_index = await _timeline_frame_cache_status_index(
        layers=resolved_layers,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        bbox=bbox,
        zoom=zoom,
    )
    days: list[dict[str, Any]] = []
    for display_date in _iter_dates(resolved_date_from, resolved_date_to):
        layer_frames: dict[str, dict[str, Any]] = {}
        for internal_layer in resolved_layers:
            frame_metadata = _frame_metadata_from_snapshot(
                internal_layer=internal_layer,
                display_date=display_date,
                snapshot_index=snapshot_index,
            )
            cache_status = cache_status_index.get(frame_metadata["layer_id"], {}).get(display_date.isoformat(), "missing")
            layer_frames[frame_metadata["layer_id"]] = {
                **frame_metadata,
                "cache_status": cache_status,
                "warm_available": cache_status == "ready",
            }
        days.append(
            {
                "display_date": display_date.isoformat(),
                "available": all(frame.get("available", False) for frame in layer_frames.values()) if layer_frames else False,
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
            now = datetime.now(row.expires_at.tzinfo) if row.expires_at.tzinfo else datetime.now(timezone.utc)
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
                expires_at=datetime.now(timezone.utc) + timedelta(hours=settings.coneat_cache_ttl_hours),
                metadata_extra={"params": normalized},
            )
            session.add(row)
        else:
            row.request_name = _coneat_request_name(normalized)
            row.content_type = content_type
            row.content = content
            row.content_hash = hashlib.sha1(content).hexdigest()
            row.expires_at = datetime.now(timezone.utc) + timedelta(hours=settings.coneat_cache_ttl_hours)
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
            now = datetime.now(row.expires_at.tzinfo) if row.expires_at.tzinfo else datetime.now(timezone.utc)
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
                expires_at=datetime.now(timezone.utc) + timedelta(hours=settings.coneat_cache_ttl_hours),
                metadata_extra={"params": params, "overlay_id": overlay_id},
            )
            session.add(row)
        else:
            row.request_name = "EXPORT"
            row.content_type = content_type
            row.content = content
            row.content_hash = hashlib.sha1(content).hexdigest()
            row.expires_at = datetime.now(timezone.utc) + timedelta(hours=settings.coneat_cache_ttl_hours)
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
