"""
AgroClimaX — API Server + Tile Service
Sirve datos reales de Copernicus al dashboard frontend.
v0.2: tile proxy para capas agronomicas (NDVI, NDMI, NDWI, SAVI, RGB, SAR)
"""
import json, asyncio, math, requests, os, httpx
from pathlib import Path
from datetime import date, timedelta

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from data_fetcher import run_pipeline, get_token, fetch_ndmi_s2, fetch_s1_stats

app = FastAPI(title="AgroClimaX API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Cache JSON ────────────────────────────────────────────────────────────────
_cache: dict = {}
CACHE_FILE = Path(__file__).parent / ".cache_agroclimax.json"

# ── Tile Cache ────────────────────────────────────────────────────────────────
TILE_CACHE_DIR = Path(__file__).parent / ".tile_cache"
TILE_CACHE_DIR.mkdir(exist_ok=True)

SH_PROCESS_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"

# Transparent 1x1 PNG para tiles sin datos
TRANSPARENT_PNG = (
    b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01'
    b'\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89'
    b'\x00\x00\x00\x0bIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00'
    b'\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82'
)

# ── Info de cada capa ─────────────────────────────────────────────────────────
CAPAS_INFO = {
    "rgb":  {"src": "sentinel-2-l2a", "clouds": True},
    "ndvi": {"src": "sentinel-2-l2a", "clouds": True},
    "ndmi": {"src": "sentinel-2-l2a", "clouds": True},
    "ndwi": {"src": "sentinel-2-l2a", "clouds": True},
    "savi": {"src": "sentinel-2-l2a", "clouds": True},
    "sar":  {"src": "sentinel-1-grd",  "clouds": False},
    "alerta_fusion": {"fusion": True, "clouds": True},
    "lst": {"src": "sentinel-3-slstr", "clouds": False},
}

# ── Evalscripts (RGBA UINT8) para cada capa agronomica ───────────────────────
EVALSCRIPTS = {

    "rgb": """//VERSION=3
function setup() {
  return { input:[{bands:["B04","B03","B02","dataMask"]}], output:{bands:4,sampleType:"UINT8"} };
}
function evaluatePixel(s) {
  if (!s.dataMask) return [0,0,0,0];
  return [
    Math.min(255,Math.round(s.B04*255*3.5)),
    Math.min(255,Math.round(s.B03*255*3.5)),
    Math.min(255,Math.round(s.B02*255*3.5)),
    255
  ];
}""",

    "ndvi": """//VERSION=3
// NDVI = (B08-B04)/(B08+B04)  |  Salud y densidad de la vegetacion
// Rojo < 0 (suelo desnudo) -> Amarillo (escasa) -> Verde intenso (densa)
function setup() {
  return { input:[{bands:["B04","B08","dataMask"]}], output:{bands:4,sampleType:"UINT8"} };
}
function evaluatePixel(s) {
  if (!s.dataMask) return [0,0,0,0];
  var v = Math.max(-1, Math.min(1, (s.B08-s.B04)/(s.B08+s.B04+1e-6)));
  var r,g,b;
  if (v < 0) {
    r = 160; g = 100; b = 60;
  } else if (v < 0.2) {
    var t = v/0.2;
    r = Math.round(160+40*t); g = Math.round(100+80*t); b = Math.round(60*(1-t));
  } else if (v < 0.5) {
    var t = (v-0.2)/0.3;
    r = Math.round(200*(1-t)+80*t); g = Math.round(180*(1-t)+180*t); b = 0;
  } else {
    var t = Math.min(1,(v-0.5)/0.5);
    r = Math.round(80*(1-t)); g = Math.round(180*(1-t)+120*t); b = 0;
  }
  return [r,g,b,255];
}""",

    "ndmi": """//VERSION=3
// NDMI = (B08-B11)/(B08+B11)  |  Humedad en follaje y canopia
// Marron (muy seco) -> Naranja (estres) -> Celeste (adecuado) -> Azul (muy humedo)
function setup() {
  return { input:[{bands:["B08","B11","dataMask"]}], output:{bands:4,sampleType:"UINT8"} };
}
function evaluatePixel(s) {
  if (!s.dataMask) return [0,0,0,0];
  var v = Math.max(-0.8, Math.min(0.8, (s.B08-s.B11)/(s.B08+s.B11+1e-6)));
  var r,g,b;
  if (v < -0.3) {
    r = 180; g = 70; b = 20;
  } else if (v < 0) {
    var t = (v+0.3)/0.3;
    r = Math.round(180*(1-t)+240*t); g = Math.round(70*(1-t)+200*t); b = Math.round(20*(1-t)+80*t);
  } else if (v < 0.3) {
    var t = v/0.3;
    r = Math.round(240*(1-t)+30*t); g = Math.round(200*(1-t)+130*t); b = Math.round(80*(1-t)+220*t);
  } else {
    r = 0; g = 80; b = 210;
  }
  return [r,g,b,255];
}""",

    "ndwi": """//VERSION=3
// NDWI = (B03-B08)/(B03+B08)  |  Agua superficial e inundaciones
// Ocre (suelo/pastizal) -> Celeste (suelo humedo) -> Azul (agua)
function setup() {
  return { input:[{bands:["B03","B08","dataMask"]}], output:{bands:4,sampleType:"UINT8"} };
}
function evaluatePixel(s) {
  if (!s.dataMask) return [0,0,0,0];
  var v = Math.max(-1, Math.min(1, (s.B03-s.B08)/(s.B03+s.B08+1e-6)));
  var r,g,b;
  if (v > 0.1) {
    r = 0; g = Math.round(80+120*Math.min(1,v)); b = 220;
  } else if (v > -0.3) {
    var t = (v+0.3)/0.4;
    r = Math.round(175*(1-t)+40*t); g = Math.round(160*(1-t)+150*t); b = Math.round(100*(1-t)+200*t);
  } else {
    r = 175; g = 155; b = 100;
  }
  return [r,g,b,255];
}""",

    "savi": """//VERSION=3
// SAVI = (B08-B04)/(B08+B04+L)*(1+L)  L=0.5  |  Vegetacion con correccion de suelo
// Ideal para pastizales y suelos desnudos frecuentes en Rivera
// Ocre (suelo) -> Verde claro (pastizal) -> Verde (vegetacion densa)
function setup() {
  return { input:[{bands:["B04","B08","dataMask"]}], output:{bands:4,sampleType:"UINT8"} };
}
function evaluatePixel(s) {
  if (!s.dataMask) return [0,0,0,0];
  var L = 0.5;
  var v = Math.max(-1, Math.min(1, ((s.B08-s.B04)/(s.B08+s.B04+L))*(1+L)));
  var r,g,b;
  if (v < 0.1) {
    r = 210; g = 175; b = 130;
  } else if (v < 0.35) {
    var t = (v-0.1)/0.25;
    r = Math.round(210*(1-t)+140*t); g = Math.round(175*(1-t)+200*t); b = Math.round(130*(1-t)+50*t);
  } else if (v < 0.6) {
    var t = (v-0.35)/0.25;
    r = Math.round(140*(1-t)+30*t); g = Math.round(200*(1-t)+160*t); b = Math.round(50*(1-t));
  } else {
    r = 20; g = 130; b = 0;
  }
  return [r,g,b,255];
}""",

    "sar": """//VERSION=3
// SAR Humedad de Suelo — Método Díaz 2026 completo
// Pipeline: VV+VH → corrección vegetación → 5 puntos calibración → humedad%
//
// Leyenda:
//   Azul       → agua libre
//   Verde osc. → vegetación densa (suelo no observable)
//   Rojo→Amar→Verde→Celeste → gradiente seco→húmedo
//
// Calibración: Rivera, Uruguay, 1/3/2026
//   P1 muy seco   VV=-16.92 dB  NDMI=-0.33
//   P2 seco        VV=-13.49 dB  NDMI=-0.11
//   P3 med         VV=-12.42 dB  NDMI= 0.07
//   P4 húmedo      VV=-10.96 dB  NDMI= 0.25
//   P5 muy húmedo  VV= -8.97 dB  NDMI= 0.44
//
// humedad% = 100 × (NDMI − (−0.5)) / (0.5 − (−0.5))
function setup() {
  return { input:[{bands:["VV","VH"],units:"LINEAR_POWER"}], output:{bands:4,sampleType:"UINT8"} };
}
// Matriz de calibración Díaz 2026
var cal = [[-16.92,-0.33],[-13.49,-0.11],[-12.42,0.07],[-10.96,0.25],[-8.97,0.44]];
// VV_suelo_dB → NDMI por interpolación lineal con extrapolación plana
function vvANdmi(vv) {
  var x=cal.map(function(p){return p[0];}), y=cal.map(function(p){return p[1];});
  for (var i=1;i<y.length;i++) if(y[i]<y[i-1]) y[i]=y[i-1]; // monotonicidad
  if (vv<=x[0]) return y[0];
  if (vv>=x[x.length-1]) return y[y.length-1];
  for (var i=0;i<x.length-1;i++) {
    if (vv>=x[i]&&vv<=x[i+1]) { var t=(vv-x[i])/(x[i+1]-x[i]); return y[i]+t*(y[i+1]-y[i]); }
  }
  return y[0];
}
// NDMI → humedad % (escala Díaz: ndmi_min=-0.5, ndmi_max=0.5)
function ndmiAHumedad(ndmi) { return Math.min(100,Math.max(0,100*(ndmi+0.5)/1.0)); }
// Color: rojo (seco) → amarillo → verde → celeste (húmedo)
function humedadAColor(h) {
  var r,g,b;
  if (h<25)      { var t=h/25;       r=220; g=Math.round(40+80*t);  b=20; }
  else if (h<50) { var t=(h-25)/25;  r=Math.round(220*(1-t)+240*t); g=Math.round(120+80*t);  b=20; }
  else if (h<75) { var t=(h-50)/25;  r=Math.round(240*(1-t)+30*t);  g=Math.round(200+20*t);  b=Math.round(20+60*t); }
  else           { var t=(h-75)/25;  r=Math.round(30*(1-t));         g=Math.round(220-60*t);  b=Math.round(80+140*t); }
  return [r,g,b,220];
}
function evaluatePixel(s) {
  var vv=s.VV, vh=s.VH;
  if (!vv||!vh||vv<=0||vh<=0) return [0,0,0,0];
  var vv_dB=10*Math.log10(vv+1e-10), vh_dB=10*Math.log10(vh+1e-10);
  // 1. Agua libre
  if (vv_dB<-18&&vh_dB<-23) return [20,80,200,200];
  // 2. Vegetación densa: suelo no observable
  var rvi=vh/(vv+vh);
  var veg=Math.min(1,Math.max(0,(rvi-0.1)/0.4));
  if (veg>0.7) return [30,110,30,180];
  // 3. Corrección de vegetación sobre VV (Díaz 2026)
  var vv_suelo_dB=vv_dB-veg*2.5;
  // 4. Calibración: vv_suelo_dB → NDMI → humedad%
  var ndmi=vvANdmi(vv_suelo_dB);
  var h=ndmiAHumedad(ndmi);
  return humedadAColor(h);
}""",


    "lst": """//VERSION=3
function setup() {
  return { input:[{bands:["S8","dataMask"]}], output:{bands:4,sampleType:"UINT8"} };
}
function evaluatePixel(s) {
  if (!s.dataMask || !s.S8) return [0,0,0,0];
  var tempC = s.S8 - 273.15;
  var t = Math.max(0, Math.min(1, (tempC - 10) / 40));
  var r, g, b;
  if (t < 0.25) { r=0; g=Math.round(t*4*255); b=255; }
  else if (t < 0.5) { r=0; g=255; b=Math.round((1-(t-0.25)*4)*255); }
  else if (t < 0.75) { r=Math.round((t-0.5)*4*255); g=255; b=0; }
  else { r=255; g=Math.round((1-(t-0.75)*4)*255); b=0; }
  return [r, g, b, 255];
}""",
    "alerta_fusion": """//VERSION=3
function setup() {
  return {
    input: [
      {datasource: "s1", bands: ["VV", "VH"]},
      {datasource: "s2", bands: ["B04", "B08", "B11", "dataMask"]}
    ],
    output: {bands: 4, sampleType: "UINT8"}
  };
}
var cal = [[-16.92,-0.33],[-13.49,-0.11],[-12.42,0.07],[-10.96,0.25],[-8.97,0.44]];
function vvANdmi(vv) {
  var x=cal.map(function(p){return p[0];}), y=cal.map(function(p){return p[1];});
  for (var i=1;i<y.length;i++) if(y[i]<y[i-1]) y[i]=y[i-1];
  if (vv<=x[0]) return y[0];
  if (vv>=x[x.length-1]) return y[y.length-1];
  for (var i=0;i<x.length-1;i++) {
    if (vv>=x[i]&&vv<=x[i+1]) { var t=(vv-x[i])/(x[i+1]-x[i]); return y[i]+t*(y[i+1]-y[i]); }
  }
  return y[0];
}
function ndmiAHumedad(ndmi) { return Math.min(100,Math.max(0,100*(ndmi+0.5)/1.0)); }

function evaluatePixel(samples) {
  var s1 = samples.s1[0];
  var s2 = samples.s2[0];
  if (!s2 || !s1 || !s2.dataMask || !s1.VV || !s1.VH || s1.VV<=0 || s1.VH<=0) return [0,0,0,0];
  var vv = s1.VV, vh = s1.VH;
  var vv_dB=10*Math.log10(vv+1e-10), vh_dB=10*Math.log10(vh+1e-10);
  if (vv_dB<-18&&vh_dB<-23) return [0,0,0,0]; // Agua libre
  var rvi=vh/(vv+vh);
  var veg=Math.min(1,Math.max(0,(rvi-0.1)/0.4));
  if (veg>0.7) return [0,0,0,0]; // Veg densa oculta suelo
  var vv_suelo_dB=vv_dB-veg*2.5;
  var hum = ndmiAHumedad(vvANdmi(vv_suelo_dB));
  var nivel_s1 = hum < 15 ? 3 : hum < 25 ? 2 : hum < 50 ? 1 : 0;
  
  var ndmi_s2 = (s2.B08 - s2.B11) / (s2.B08 + s2.B11 + 1e-6);
  var nivel_s2 = ndmi_s2 < -0.10 ? 3 : ndmi_s2 < 0 ? 2 : ndmi_s2 < 0.10 ? 1 : 0;
  
  var nivel = Math.max(nivel_s1, nivel_s2);
  
  if (nivel === 0) return [46,  204, 113, 180];
  if (nivel === 1) return [241, 196, 15,  180];
  if (nivel === 2) return [230, 126, 34,  180];
  if (nivel === 3) return [231, 76,  60,  180];
  return [0,0,0,0];
}""",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def tile_to_bbox(z: int, x: int, y: int) -> list:
    """Convierte tile Slippy Map (z/x/y) a bbox WGS84 [W, S, E, N]."""
    n = 2 ** z
    return [
        round(x / n * 360.0 - 180.0, 6),
        round(math.degrees(math.atan(math.sinh(math.pi * (1 - 2*(y+1)/n)))), 6),
        round((x+1) / n * 360.0 - 180.0, 6),
        round(math.degrees(math.atan(math.sinh(math.pi * (1 - 2*y/n)))), 6),
    ]


def load_cache():
    global _cache
    if CACHE_FILE.exists():
        try:
            _cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            _cache = {}


def save_cache():
    CACHE_FILE.write_text(
        json.dumps(_cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


load_cache()


def _calcular_deficit_desde_cache(cache: dict) -> dict:
    """
    Estima días en déficit consecutivos desde la serie histórica cacheada.
    Si no hay histórico, usa solo el dato actual.
    """
    serie = cache.get("serie_historica", [])
    if not serie:
        # Solo tenemos el dato actual
        humedad = cache.get("resumen", {}).get("humedad_s1_pct", 100)
        nivel_actual = 3 if humedad < 15 else 2 if humedad < 25 else 1 if humedad < 50 else 0
        dias = 5 if nivel_actual >= 1 else 0
        return {"dias_deficit": dias, "es_prolongada": False}

    periodos = 0
    for punto in reversed(serie):  # de más reciente a más antiguo
        if punto.get("nivel", 0) >= 1:
            periodos += 1
        else:
            break

    dias = periodos * 5
    return {"dias_deficit": dias, "es_prolongada": dias >= 25}


# ── Endpoints JSON ────────────────────────────────────────────────────────────

@app.get("/api/estado-actual")
async def estado_actual():
    """Estado hidrico actual de Rivera — datos reales Copernicus."""
    hoy = str(date.today())
    if _cache.get("fecha") == hoy and "resumen" in _cache:
        return JSONResponse(_cache)
    try:
        resultado = await asyncio.to_thread(run_pipeline)
        _cache.update(resultado)
        # Calcular deficit consecutivo desde cache (usa serie_historica si existe)
        try:
            deficit_info = _calcular_deficit_desde_cache(_cache)
            resultado.update(deficit_info)
            _cache.update(deficit_info)
        except Exception:
            resultado.setdefault("dias_deficit", 0)
            resultado.setdefault("es_prolongada", False)
        save_cache()
        return JSONResponse(resultado)
    except Exception as e:
        if _cache:
            _cache["advertencia"] = f"Error actualizando: {str(e)[:100]}"
            return JSONResponse(_cache)
        return JSONResponse({"error": str(e)}, status_code=500)



# Import Request movido al inicio

@app.post("/api/stats/custom")
async def stats_custom(request: Request):
    try:
        geom = await request.json()
        resultado = await asyncio.to_thread(run_pipeline, geom)
        try:
            deficit_info = _calcular_deficit_desde_cache(_cache)
            resultado.update(deficit_info)
        except Exception:
            resultado.setdefault("dias_deficit", 0)
            resultado.setdefault("es_prolongada", False)
        return JSONResponse(resultado)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/historico")
async def historico(dias: int = 30):
    """Serie temporal de NDMI y humedad — ultimos N dias."""
    hoy = date.today()
    serie = []
    try:
        token = await asyncio.to_thread(get_token)
        n_periodos = min(dias // 5, 6)
        for i in range(n_periodos):
            fecha_fin    = hoy - timedelta(days=i * 5)
            fecha_inicio = fecha_fin - timedelta(days=10)
            s2 = await asyncio.to_thread(fetch_ndmi_s2, token, str(fecha_inicio), str(fecha_fin))
            s1 = await asyncio.to_thread(fetch_s1_stats, token, str(fecha_inicio), str(fecha_fin))
            ndmi    = s2.get("ndmi_media") if "error" not in s2 else None
            humedad = s1.get("humedad_media") if "error" not in s1 else None
            if ndmi is not None or humedad is not None:
                nivel = 0
                if humedad is not None:
                    nivel = 3 if humedad < 15 else 2 if humedad < 25 else 1 if humedad < 50 else 0
                serie.append({
                    "fecha":       str(fecha_fin),
                    "humedad_pct": humedad,
                    "ndmi":        ndmi,
                    "nivel":       nivel,
                })
        serie.sort(key=lambda x: x["fecha"])
        # Save serie to cache so deficit calculation can use it
        _cache["serie_historica"] = serie
        save_cache()
        return JSONResponse({"departamento": "Rivera", "datos": serie})
    except Exception as e:
        return JSONResponse({"error": str(e), "datos": []}, status_code=500)


@app.get("/api/proxy/coneat")
async def proxy_coneat(request: Request):
    """
    Proxy para el WMS de CONEAT (MGAP).
    Necesario porque el MGAP solo sirve via HTTP y Railway usa HTTPS (Mixed Content error).
    """
    params = dict(request.query_params)
    url = "http://dgrn.mgap.gub.uy/arcgis/services/TEMATICOS/IntConeat/MapServer/WMSServer"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params, timeout=30)
            return Response(
                content=resp.content,
                media_type=resp.headers.get("content-type"),
                headers={"Cache-Control": "max-age=86400", "Access-Control-Allow-Origin": "*"}
            )
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)



@app.get("/api/tiles/{layer}/{z}/{x}/{y}.png")
async def serve_tile(layer: str, z: int, x: int, y: int):
    """
    Sirve tiles PNG de imagenes satelitales reales Sentinel-1/2.
    Capas: rgb, ndvi, ndmi, ndwi, savi, sar
    Cachea en disco (renovacion diaria).
    """
    if layer not in EVALSCRIPTS:
        return Response(status_code=404)

    # Verificar rango de zoom util (Sentinel-2 = 10m, Sentinel-1 = 10m)
    if z < 7 or z > 13:
        return Response(content=TRANSPARENT_PNG, media_type="image/png",
                        headers={"Access-Control-Allow-Origin": "*"})

    # Cache diario
    today = str(date.today())
    cache_path = TILE_CACHE_DIR / f"{layer}_{today}_{z}_{x}_{y}.png"
    if cache_path.exists():
        return Response(
            content=cache_path.read_bytes(),
            media_type="image/png",
            headers={"Cache-Control": "max-age=7200", "Access-Control-Allow-Origin": "*"},
        )

    info = CAPAS_INFO[layer]
    bbox = tile_to_bbox(z, x, y)
    hoy  = str(date.today())
    inicio = str(date.today() - timedelta(days=45))

    data_filter = {
        "timeRange": {"from": f"{inicio}T00:00:00Z", "to": f"{hoy}T23:59:59Z"}
    }
    if info.get("clouds"):
        data_filter["maxCloudCoverage"] = 50

    if info.get("fusion"):
        data_sources = [
            {
                "id": "s1",
                "type": "sentinel-1-grd",
                "dataFilter": {"timeRange": {"from": f"{inicio}T00:00:00Z", "to": f"{hoy}T23:59:59Z"}}
            },
            {
                "id": "s2",
                "type": "sentinel-2-l2a",
                "dataFilter": data_filter
            }
        ]
    else:
        data_sources = [{"type": info.get("src"), "dataFilter": data_filter}]

    payload = {
        "input": {
            "bounds": {
                "bbox": bbox,
                "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"},
            },
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
        token = await asyncio.to_thread(get_token)
        resp  = await asyncio.to_thread(
            lambda: requests.post(
                SH_PROCESS_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json",
                    "Accept":        "image/png",
                },
                timeout=30,
            )
        )
        if resp.status_code == 200 and "image" in resp.headers.get("content-type", ""):
            cache_path.write_bytes(resp.content)
            return Response(
                content=resp.content,
                media_type="image/png",
                headers={"Cache-Control": "max-age=7200", "Access-Control-Allow-Origin": "*"},
            )
    except Exception:
        pass

    # Sin datos: tile transparente (sin error visible en Leaflet)
    return Response(
        content=TRANSPARENT_PNG,
        media_type="image/png",
        headers={"Access-Control-Allow-Origin": "*"},
    )


GADM_RIVERA_CACHE = Path(__file__).parent / ".geojson_rivera.json"
# Nivel 1 = departamentos de Uruguay (límite del departamento completo)
GADM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_URY_1.json"

@app.get("/api/geojson/rivera")
async def geojson_rivera():
    """
    Devuelve el GeoJSON del límite administrativo real del departamento de Rivera (GADM 4.1 nivel 1).
    Cachea en disco indefinidamente (los límites no cambian).
    """
    # Usar cache si existe
    if GADM_RIVERA_CACHE.exists():
        return JSONResponse(json.loads(GADM_RIVERA_CACHE.read_text(encoding="utf-8")))

    try:
        # Descargar GADM Uruguay nivel 1 (departamentos)
        resp = await asyncio.to_thread(
            lambda: requests.get(GADM_URL, timeout=60)
        )
        if resp.status_code != 200:
            raise ValueError(f"GADM HTTP {resp.status_code}")

        gadm = resp.json()

        # En nivel 1: NAME_1 = nombre del departamento
        rivera_feature = None
        for feature in gadm.get("features", []):
            props = feature.get("properties", {})
            if props.get("NAME_1", "").lower() == "rivera":
                rivera_feature = feature
                break

        if not rivera_feature:
            raise ValueError("Departamento Rivera no encontrado en GADM nivel 1")

        geojson = {
            "type": "FeatureCollection",
            "features": [rivera_feature]
        }

        GADM_RIVERA_CACHE.write_text(json.dumps(geojson), encoding="utf-8")
        return JSONResponse(geojson)

    except Exception as e:
        # Fallback: polígono simplificado del límite real de Rivera
        fallback = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"NAME_2": "Rivera", "fuente": "fallback-aproximado"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-57.642, -30.145], [-56.824, -30.000], [-55.971, -30.110],
                        [-55.198, -30.257], [-54.610, -30.383], [-54.178, -30.695],
                        [-53.777, -31.076], [-54.018, -31.514], [-54.572, -31.828],
                        [-55.103, -31.976], [-55.739, -31.857], [-56.368, -31.720],
                        [-56.969, -31.486], [-57.430, -31.165], [-57.642, -30.145]
                    ]]
                }
            }]
        }
        return JSONResponse(fallback)


@app.get("/api/health")
async def health():
    return {"status": "ok", "sistema": "AgroClimaX", "version": "0.2.0"}


# ── Frontend estático ─────────────────────────────────────────────────────────
# En Docker/Railway el frontend se copia a /frontend.
# En desarrollo local está en ../../frontend respecto a server.py.
# Import os movido al inicio
_FRONTEND_DOCKER = Path("/frontend")
_FRONTEND_LOCAL  = Path(__file__).resolve().parent.parent / "frontend"
_FRONTEND = _FRONTEND_DOCKER if _FRONTEND_DOCKER.exists() else _FRONTEND_LOCAL

if _FRONTEND.exists():
    app.mount("/static", StaticFiles(directory=str(_FRONTEND)), name="static")

    @app.get("/", include_in_schema=False)
    async def root():
        return FileResponse(str(_FRONTEND / "index.html"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8005))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
