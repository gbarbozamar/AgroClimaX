from __future__ import annotations

import asyncio
import json
import math
from datetime import date, timedelta
from pathlib import Path

import httpx
import requests

from app.core.config import settings

try:
    from data_fetcher import get_token as legacy_get_token
except Exception:  # pragma: no cover
    legacy_get_token = None


SH_PROCESS_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"
TILE_CACHE_DIR = Path(__file__).resolve().parents[2] / ".tile_cache"
TILE_CACHE_DIR.mkdir(exist_ok=True)
GADM_RIVERA_CACHE = Path(__file__).resolve().parents[2] / ".geojson_rivera.json"
GADM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_URY_1.json"

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


async def fetch_tile_png(layer: str, z: int, x: int, y: int) -> bytes:
    if layer not in EVALSCRIPTS or z < 7 or z > 13:
        return TRANSPARENT_PNG

    cache_path = TILE_CACHE_DIR / f"{layer}_{date.today()}_{z}_{x}_{y}.png"
    if cache_path.exists():
        return cache_path.read_bytes()

    if legacy_get_token is None or not settings.copernicus_enabled:
        return TRANSPARENT_PNG

    info = CAPAS_INFO[layer]
    bbox = tile_to_bbox(z, x, y)
    today = str(date.today())
    start = str(date.today() - timedelta(days=45))
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


async def proxy_coneat_request(params: dict) -> tuple[bytes, str]:
    url = "http://dgrn.mgap.gub.uy/arcgis/services/TEMATICOS/IntConeat/MapServer/WMSServer"
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url, params=params)
    return response.content, response.headers.get("content-type", "image/png")
