"""
AgroClimaX — Data Fetcher Real
Descarga datos reales de Copernicus CDSE:
  - Sentinel-2 L2A → NDMI (humedad vegetación)
  - Sentinel-1 GRD  → Retrodispersión SAR (humedad suelo)
  - ERA5 CDS        → Precipitación histórica → SPI-30
"""

import os, json, httpx, numpy as np
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# ── Credenciales ─────────────────────────────────────────────
CLIENT_ID     = os.getenv("COPERNICUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("COPERNICUS_CLIENT_SECRET")
CDS_API_KEY   = os.getenv("CDS_API_KEY")

# ── URLs CDSE ────────────────────────────────────────────────
TOKEN_URL   = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
PROCESS_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"
CATALOG_URL = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
CDS_URL     = os.getenv("CDS_API_URL", "https://cds.climate.copernicus.eu/api")

# ── AOI Rivera, Uruguay ──────────────────────────────────────
# BBox más ajustada al departamento de Rivera
RIVERA_BBOX = [-56.5, -31.5, -54.5, -30.0]  # [W, S, E, N]

def get_token() -> str:
    """Obtiene token OAuth2 de CDSE."""
    r = httpx.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def buscar_escenas_s2(token: str, dias: int = 10) -> list:
    """Busca escenas Sentinel-2 disponibles para Rivera en los últimos N días."""
    fecha_fin   = date.today()
    fecha_inicio = fecha_fin - timedelta(days=dias)

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "collections": ["sentinel-2-l2a"],
        "bbox":        RIVERA_BBOX,
        "datetime":    f"{fecha_inicio}T00:00:00Z/{fecha_fin}T23:59:59Z",
        "limit":       5,
        "fields":      {"include": ["properties.datetime", "properties.eo:cloud_cover"]},
        "filter":      {"op": "<=", "args": [{"property": "eo:cloud_cover"}, 50]}
    }

    r = httpx.post(CATALOG_URL, headers=headers, json=body, timeout=30)
    if r.status_code != 200:
        return []

    features = r.json().get("features", [])
    return [
        {
            "fecha": f["properties"]["datetime"][:10],
            "nubes": f["properties"].get("eo:cloud_cover", 0)
        }
        for f in features
    ]


def fetch_ndmi_s2(token: str, fecha_inicio: str, fecha_fin: str, geom: dict = None) -> dict:
    """
    Descarga estadísticas NDMI reales para Rivera (Sentinel-2 L2A).
    Usa Statistical API para obtener percentiles sin descargar raster completo.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    body = {
        "input": {
            "bounds": {"geometry": geom, "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}} if geom else {"bbox": RIVERA_BBOX, "properties": {"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}},
            "data": [{
                "dataFilter": {
                    "timeRange": {"from": f"{fecha_inicio}T00:00:00Z", "to": f"{fecha_fin}T23:59:59Z"},
                    "maxCloudCoverage": 50,
                    "mosaickingOrder": "leastCC"
                },
                "type": "sentinel-2-l2a"
            }]
        },
        "aggregation": {
            "timeRange": {"from": f"{fecha_inicio}T00:00:00Z", "to": f"{fecha_fin}T23:59:59Z"},
            "aggregationInterval": {"of": "P10D"},
            "resx": 0.002, "resy": 0.002,
            "evalscript": """
//VERSION=3
// NDMI = (B08-B11)/(B08+B11) — Método Díaz 2026
// Filtra píxeles inválidos via SCL (Scene Classification Layer):
//   SCL 1 = saturado/defectivo
//   SCL 3 = sombra de nube
//   SCL 6 = agua superficial
//   SCL 8 = nubes medianas
//   SCL 9 = nubes altas
//   SCL 10 = cirrus
// Solo se procesa vegetación (SCL 4) y suelo desnudo (SCL 5)
function setup() {
  return {
    input: [{ bands: ["B08", "B11", "SCL"] }],
    output: [
      { id: "default",  bands: 1, sampleType: "FLOAT32" },
      { id: "dataMask", bands: 1, sampleType: "UINT8"   }
    ]
  };
}
function evaluatePixel(s) {
  // Filtro SCL: excluir saturados, sombras, agua, nubes
  var scl = s.SCL;
  if (scl === 1 || scl === 3 || scl === 6 || scl === 8 || scl === 9 || scl === 10)
    return { default: [NaN], dataMask: [0] };
  if (!s.B08 || !s.B11 || (s.B08 + s.B11) === 0)
    return { default: [NaN], dataMask: [0] };
  var ndmi = (s.B08 - s.B11) / (s.B08 + s.B11 + 1e-6);
  return { default: [ndmi], dataMask: [1] };
}
"""
        },
        "calculations": {
            "default": {
                "statistics": {
                    "default": {
                        "percentiles": {"k": [10, 25, 50, 75, 90]},
                        "noDataValue": None
                    }
                }
            }
        }
    }

    STAT_URL = "https://sh.dataspace.copernicus.eu/api/v1/statistics"
    r = httpx.post(STAT_URL, headers=headers, json=body, timeout=60)

    if r.status_code != 200:
        return {"error": r.text, "fuente": "sentinel-2-l2a"}

    datos = r.json()
    intervalos = datos.get("data", [])

    if not intervalos:
        return {"error": "Sin datos en el período", "fuente": "sentinel-2-l2a"}

    ultimo = intervalos[-1]["outputs"]["default"]["bands"]["B0"]["stats"]
    return {
        "fuente":       "sentinel-2-l2a",
        "fecha_inicio": fecha_inicio,
        "fecha_fin":    fecha_fin,
        "ndmi_media":   round(ultimo.get("mean", 0), 4),
        "ndmi_p10":     round(ultimo["percentiles"].get("10.0", 0), 4),
        "ndmi_p25":     round(ultimo["percentiles"].get("25.0", 0), 4),
        "ndmi_p50":     round(ultimo["percentiles"].get("50.0", 0), 4),
        "ndmi_p75":     round(ultimo["percentiles"].get("75.0", 0), 4),
        "ndmi_p90":     round(ultimo["percentiles"].get("90.0", 0), 4),
        "cobertura_pct": round(100 * (1 - ultimo.get("noDataCount", 0) /
                          max(ultimo.get("sampleCount", 1), 1)), 1),
    }


def fetch_s1_stats(token: str, fecha_inicio: str, fecha_fin: str, geom: dict = None) -> dict:
    """
    Descarga estadísticas de Sentinel-1 para Rivera.
    Implementa el método Díaz 2026 completo por píxel:
      - VV + VH en LINEAR_POWER
      - Detección de agua libre (VV y VH muy bajos)
      - Estimación de vegetación via RVI = VH/(VV+VH)
      - Exclusión de vegetación densa (suelo no observable, RVI > 0.7)
      - Corrección de vegetación: vv_suelo_dB = vv_dB - veg * 2.5
    Las estadísticas resultantes son sobre vv_suelo_dB, ya corregido.
    """
    STAT_URL = "https://sh.dataspace.copernicus.eu/api/v1/statistics"
    headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Evalscript fiel al método Díaz 2026:
    # retorna vv_suelo_dB (VV corregido por vegetación), excluyendo agua y veg densa
    evalscript_vv = """
//VERSION=3
// Método Díaz 2026 — pipeline completo por píxel
// Salida: vv_suelo_dB (VV corregido por vegetación, sin agua, sin veg densa)
function setup() {
  return {
    input: [{ bands: ["VV", "VH"], units: "LINEAR_POWER" }],
    output: [
      { id: "default",  bands: 1, sampleType: "FLOAT32" },
      { id: "dataMask", bands: 1, sampleType: "UINT8"   }
    ]
  };
}
function evaluatePixel(s) {
  var vv = s.VV; var vh = s.VH;
  if (!vv || !vh || vv <= 0 || vh <= 0) return { default: [NaN], dataMask: [0] };
  var vv_dB = 10 * Math.log10(vv);
  var vh_dB = 10 * Math.log10(vh);
  // Agua libre: retrodispersión especular muy baja en ambas polarizaciones
  if (vv_dB < -18 && vh_dB < -23) return { default: [NaN], dataMask: [0] };
  // Vegetación densa: suelo no observable
  var rvi = vh / (vv + vh);
  var veg = Math.min(1, Math.max(0, (rvi - 0.1) / 0.4));
  if (veg > 0.7) return { default: [NaN], dataMask: [0] };
  // Corrección de vegetación sobre VV
  var vv_suelo_dB = vv_dB - veg * 2.5;
  return { default: [vv_suelo_dB], dataMask: [1] };
}
"""

    body = {
        "input": {
            "bounds": {"geometry": geom, "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}} if geom else {"bbox": RIVERA_BBOX, "properties": {"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}},
            "data": [{
                "dataFilter": {
                    "timeRange": {"from": f"{fecha_inicio}T00:00:00Z", "to": f"{fecha_fin}T23:59:59Z"},
                    "acquisitionMode": "IW",
                    "polarization": "DV"
                },
                "type": "sentinel-1-grd"
            }]
        },
        "aggregation": {
            "timeRange": {"from": f"{fecha_inicio}T00:00:00Z", "to": f"{fecha_fin}T23:59:59Z"},
            "aggregationInterval": {"of": "P6D"},
            "resx": 0.002, "resy": 0.002,
            "evalscript": evalscript_vv
        },
        "calculations": {
            "default": {
                "statistics": {
                    "default": {
                        "percentiles": {"k": [10, 25, 50, 75, 90]},
                        "noDataValue": None
                    }
                }
            }
        }
    }

    r = httpx.post(STAT_URL, headers=headers, json=body, timeout=60)

    if r.status_code != 200:
        return {"error": r.text, "fuente": "sentinel-1-grd"}

    datos = r.json()
    intervalos = datos.get("data", [])

    if not intervalos:
        return {"error": "Sin datos S1 en el período", "fuente": "sentinel-1-grd"}

    ultimo = intervalos[-1]["outputs"]["default"]["bands"]["B0"]["stats"]
    # vv_suelo_db ya viene corregido por vegetación (evalscript aplica Díaz 2026 por píxel)
    vv_db_media = ultimo.get("mean", -13.0)

    # Calibración Díaz 2026: 5 puntos Rivera 1/3/2026
    # vv_suelo_dB → NDMI (interpolación lineal) → humedad %
    puntos = [(-16.92, -0.33), (-13.49, -0.11), (-12.42, 0.07), (-10.96, 0.25), (-8.97, 0.44)]
    xs = [p[0] for p in puntos]
    ys = [p[1] for p in puntos]

    def interpolar(vv):
        if vv <= xs[0]:  return ys[0]
        if vv >= xs[-1]: return ys[-1]
        for i in range(len(xs)-1):
            if xs[i] <= vv <= xs[i+1]:
                t = (vv - xs[i]) / (xs[i+1] - xs[i])
                return ys[i] + t * (ys[i+1] - ys[i])
        return ys[0]

    def ndmi_a_humedad(ndmi):
        return max(0, min(100, 100 * (ndmi + 0.5) / 1.0))

    ndmi_estimado  = interpolar(vv_db_media)
    humedad_media  = ndmi_a_humedad(ndmi_estimado)

    ndmi_p10 = interpolar(ultimo["percentiles"].get("10.0", vv_db_media))
    ndmi_p90 = interpolar(ultimo["percentiles"].get("90.0", vv_db_media))

    # Calcular % área bajo estrés usando distribución de percentiles
    h_p10 = ndmi_a_humedad(interpolar(ultimo["percentiles"].get("10.0", vv_db_media)))
    h_p25 = ndmi_a_humedad(interpolar(ultimo["percentiles"].get("25.0", vv_db_media)))
    h_p50 = ndmi_a_humedad(interpolar(ultimo["percentiles"].get("50.0", vv_db_media)))
    h_p75 = ndmi_a_humedad(interpolar(ultimo["percentiles"].get("75.0", vv_db_media)))
    h_p90 = ndmi_a_humedad(interpolar(ultimo["percentiles"].get("90.0", vv_db_media)))

    UMBRAL_ALERTA = 50.0  # % humedad umbral VERDE/AMARILLO

    pct_bajo_estres = 0.0
    if h_p90 <= UMBRAL_ALERTA:
        pct_bajo_estres = 90.0
    elif h_p10 >= UMBRAL_ALERTA:
        pct_bajo_estres = 10.0
    elif h_p50 <= UMBRAL_ALERTA:
        # Entre p50 y p75
        t = (UMBRAL_ALERTA - h_p50) / max(h_p75 - h_p50, 1.0)
        pct_bajo_estres = 50.0 + t * 25.0
    elif h_p25 <= UMBRAL_ALERTA:
        # Entre p25 y p50
        t = (UMBRAL_ALERTA - h_p25) / max(h_p50 - h_p25, 1.0)
        pct_bajo_estres = 25.0 + t * 25.0
    else:
        # Entre p10 y p25
        t = (UMBRAL_ALERTA - h_p10) / max(h_p25 - h_p10, 1.0)
        pct_bajo_estres = 10.0 + t * 15.0

    return {
        "fuente":                "sentinel-1-grd",
        "fecha_inicio":          fecha_inicio,
        "fecha_fin":             fecha_fin,
        "vv_suelo_db_media":     round(vv_db_media, 3),  # ya corregido por vegetación
        "ndmi_estimado":         round(ndmi_estimado, 4),
        "humedad_media":         round(humedad_media, 1),
        "humedad_p10":           round(ndmi_a_humedad(ndmi_p10), 1),
        "humedad_p90":           round(ndmi_a_humedad(ndmi_p90), 1),
        "cobertura_pct":         round(100 * (1 - ultimo.get("noDataCount", 0) /
                                 max(ultimo.get("sampleCount", 1), 1)), 1),
        "pct_area_bajo_estres":  round(min(100.0, max(0.0, pct_bajo_estres)), 1),
        "calibracion":           "Diaz_Rivera_2026_5pts"
    }


def fetch_precipitacion_openmeteo(lat: float = -31.5, lon: float = -55.5, dias: int = 730) -> dict:
    """
    Descarga precipitación diaria histórica para Rivera desde Open-Meteo (ERA5-Land).
    Sin autenticación, respuesta en <2s, datos desde 1940.
    """
    fecha_fin = date.today()
    fecha_inicio = fecha_fin - timedelta(days=dias)

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={fecha_inicio}&end_date={fecha_fin}"
        f"&daily=precipitation_sum&timezone=America%2FMontevideo"
    )

    r = httpx.get(url, timeout=30)
    if r.status_code != 200:
        return {"error": f"Open-Meteo HTTP {r.status_code}"}

    data = r.json()
    fechas = data["daily"]["time"]
    precip = data["daily"]["precipitation_sum"]

    # Replace None with 0.0
    precip = [p if p is not None else 0.0 for p in precip]

    return {"fechas": fechas, "precipitacion_mm": precip}


def calcular_spi_30(precip_data: dict, lat: float, lon: float) -> dict:
    """
    Calcula SPI-30 (Standardized Precipitation Index, ventana 30 días)
    usando precipitación diaria de Open-Meteo.
    """
    if "error" in precip_data:
        return {"error": precip_data["error"], "spi_30d": 0.0, "spi_categoria": "Sin datos", "lat": lat, "lon": lon}

    fechas = precip_data["fechas"]
    precip = precip_data["precipitacion_mm"]

    if len(precip) < 60:
        return {"error": "Insuficientes datos históricos", "spi_30d": 0.0, "spi_categoria": "Sin datos", "lat": lat, "lon": lon}

    # Calcular sumas acumuladas de 30 días
    sumas_30d = []
    meses_30d = []
    for i in range(29, len(precip)):
        s = sum(precip[i-29:i+1])
        sumas_30d.append(s)
        meses_30d.append(int(fechas[i][5:7]))  # month number

    # Climatología mensual desde el histórico
    from collections import defaultdict
    por_mes = defaultdict(list)
    for s, m in zip(sumas_30d, meses_30d):
        por_mes[m].append(s)

    # SPI del período más reciente
    mes_actual = int(fechas[-1][5:7])
    datos_mes = por_mes.get(mes_actual, [])

    if len(datos_mes) < 2:
        return {"spi_30d": 0.0, "spi_categoria": "Normal", "fuente": "open-meteo-era5land", "lat": lat, "lon": lon, "nota": "Datos insuficientes"}

    media = sum(datos_mes) / len(datos_mes)
    variance = sum((x - media) ** 2 for x in datos_mes) / (len(datos_mes) - 1)
    std = variance ** 0.5

    if std < 1.0:
        std = 1.0  # evitar division por cero

    precip_30d_actual = sumas_30d[-1]
    spi = (precip_30d_actual - media) / std
    spi = max(-3.0, min(3.0, round(spi, 3)))

    return {
        "fuente": "open-meteo-era5land",
        "lat": round(lat, 5),
        "lon": round(lon, 5),
        "precip_30d_mm": round(precip_30d_actual, 1),
        "media_historica_30d_mm": round(media, 1),
        "std_historica_mm": round(std, 1),
        "spi_30d": spi,
        "spi_categoria": clasificar_spi(spi),
        "n_anios_historico": round(len(por_mes.get(mes_actual, [])), 0),
    }


def fetch_era5_precipitacion(lat: float, lon: float) -> dict:
    """
    Obtiene SPI-30 real para coordenadas específicas via Open-Meteo (ERA5-Land).
    """
    try:
        precip_data = fetch_precipitacion_openmeteo(lat, lon)
        if "error" in precip_data:
            raise ValueError(precip_data["error"])
        return calcular_spi_30(precip_data, lat, lon)
    except Exception as e:
        # Fallback: SPI neutro con nota de error, NO un valor hardcodeado
        return {
            "fuente": "fallback-sin-datos",
            "spi_30d": 0.0,
            "spi_categoria": clasificar_spi(0.0),
            "error": str(e)[:120],
            "nota": "No se pudo calcular SPI. Verificar conexión a Open-Meteo."
        }


def clasificar_spi(spi: float) -> str:
    if spi >= 2.0:  return "Extremadamente húmedo"
    if spi >= 1.5:  return "Muy húmedo"
    if spi >= 1.0:  return "Moderadamente húmedo"
    if spi > -1.0:  return "Normal"
    if spi > -1.5:  return "Moderadamente seco"
    if spi > -2.0:  return "Severamente seco"
    return "Extremadamente seco"


def clasificar_alerta(humedad: float, ndmi: float, spi: float) -> dict:
    """Motor de alertas combinado S1 + S2 + SPI."""
    NIVELES = {
        "VERDE":    {"color": "#2ecc71", "codigo": 0},
        "AMARILLO": {"color": "#f1c40f", "codigo": 1},
        "NARANJA":  {"color": "#e67e22", "codigo": 2},
        "ROJO":     {"color": "#e74c3c", "codigo": 3},
    }
    DESCRIPCIONES = {
        "VERDE":    "Condiciones hídricas normales. Sin déficit.",
        "AMARILLO": "Inicio de déficit hídrico. Monitoreo reforzado.",
        "NARANJA":  "Déficit hídrico moderado. Estrés en cultivos y pasturas.",
        "ROJO":     "Emergencia hídrica severa. Riesgo crítico para ganadería y agricultura.",
    }
    ACCIONES = {
        "VERDE":    "Monitoreo rutinario.",
        "AMARILLO": "Verificar fuentes de agua. Evaluar riego suplementario.",
        "NARANJA":  "Activar protocolos de emergencia agropecuaria.",
        "ROJO":     "Notificar MGAP. Activar declaración de emergencia agropecuaria.",
    }

    # Clasificar por humedad S1
    if   humedad >= 50: nivel_s1 = "VERDE"
    elif humedad >= 25: nivel_s1 = "AMARILLO"
    elif humedad >= 15: nivel_s1 = "NARANJA"
    else:               nivel_s1 = "ROJO"

    # Clasificar por NDMI S2
    if   ndmi >= 0.10: nivel_s2 = "VERDE"
    elif ndmi >= 0.00: nivel_s2 = "AMARILLO"
    elif ndmi >= -0.10: nivel_s2 = "NARANJA"
    else:               nivel_s2 = "ROJO"

    # Tomar el peor nivel
    orden  = ["VERDE","AMARILLO","NARANJA","ROJO"]
    nivel  = orden[max(orden.index(nivel_s1), orden.index(nivel_s2))]

    # Reforzar con SPI si hay sequía severa
    if spi < -1.5 and orden.index(nivel) < orden.index("NARANJA"):
        nivel = "NARANJA"

    return {
        "nivel":         nivel,
        "codigo":        NIVELES[nivel]["codigo"],
        "color":         NIVELES[nivel]["color"],
        "descripcion":   DESCRIPCIONES[nivel],
        "accion":        ACCIONES[nivel],
        "nivel_s1":      nivel_s1,
        "nivel_s2":      nivel_s2,
    }


def run_pipeline(geom: dict = None) -> dict:
    """Ejecuta el pipeline completo y retorna estado hídrico actual."""
    print("Obteniendo token CDSE...")
    token = get_token()

    hoy          = date.today()
    fecha_fin    = str(hoy)
    fecha_inicio = str(hoy - timedelta(days=20))

    print(f"Descargando NDMI S2 ({fecha_inicio} a {fecha_fin})...")
    s2 = fetch_ndmi_s2(token, fecha_inicio, fecha_fin, geom)

    print(f"Descargando S1 SAR ({fecha_inicio} a {fecha_fin})...")
    s1 = fetch_s1_stats(token, fecha_inicio, fecha_fin, geom)

    print("Obteniendo datos climáticos ERA5...")
    lat, lon = (-31.5, -55.5) # Rivera centro por defecto
    if geom:
        coords = geom.get("coordinates", [[[]]])[0]
        if len(coords) > 3:
            # Centroide simple: promedio de puntos (sin contar el ultimo duplicado si existe)
            puntos = coords[:-1] if coords[0] == coords[-1] else coords
            lat = sum(c[1] for c in puntos) / len(puntos)
            lon = sum(c[0] for c in puntos) / len(puntos)
    
    print(f"-> Coordenadas para análisis ERA5: {lat:.5f}, {lon:.5f}")
    era5 = fetch_era5_precipitacion(lat, lon)

    if "error" in s2:
        raise Exception(f"Copernicus rechazó el Polígono para S2: {s2['error']}")
    ndmi = s2.get("ndmi_media", -0.05)
    if "error" in s1:
        raise Exception(f"Copernicus rechazó el Polígono para S1: {s1['error']}")
    humedad = s1.get("humedad_media", 30.0)
    spi      = era5.get("spi_30d", 0.0)

    alerta = clasificar_alerta(humedad, ndmi, spi)

    return {
        "fecha":         fecha_fin,
        "departamento":  "Rivera",
        "alerta":        alerta,
        "sentinel_2":    s2,
        "sentinel_1":    s1,
        "era5":          era5,
        "resumen": {
            "humedad_s1_pct": humedad,
            "ndmi_s2":        ndmi,
            "spi_30d":        spi,
            "spi_categoria":  era5.get("spi_categoria", "Sin datos"),
            "nivel":          alerta["nivel"],
            "color":          alerta["color"],
        }
    }


if __name__ == "__main__":
    import json
    resultado = run_pipeline()
    print("\n=== RESULTADO PIPELINE ===")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
