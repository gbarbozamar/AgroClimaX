"""
Ingesta de imágenes Sentinel-1 GRD desde Copernicus Data Space.
Estima humedad superficial del suelo (~5 cm) mediante retrodispersión
radar VV/VH calibrada con 5 puntos de referencia en Rivera.
"""
import logging
from datetime import date
from pathlib import Path
import numpy as np

from sentinelhub import (
    SentinelHubRequest,
    DataCollection,
    MimeType,
    CRS,
    BBox,
    SHConfig,
    bbox_to_dimensions,
)

from app.copernicus.auth import get_sentinelhub_config
from app.copernicus.aoi import get_rivera_bbox

logger = logging.getLogger(__name__)

RESOLUTION = 20  # metros

EVALSCRIPT_S1 = Path(__file__).parent.parent.parent.parent.parent / "evalscripts" / "humedad_suelo_s1.js"

# Valores especiales de salida UINT8
VALOR_VEG_DENSA = 254
VALOR_AGUA_LIBRE = 255

# Puntos de calibración (Díaz, 2026)
PUNTOS_CALIBRACION = [
    (-16.92, -0.33),  # P1 suelo muy seco
    (-13.49, -0.11),  # P2 suelo seco
    (-12.42,  0.07),  # P3 humedad media
    (-10.96,  0.25),  # P4 suelo húmedo
    ( -8.97,  0.44),  # P5 suelo muy húmedo
]


def uint8_a_porcentaje(valor: np.ndarray) -> np.ndarray:
    """
    Convierte valores UINT8 del evalscript a porcentaje de humedad.
    valor / 2.54 = % humedad
    Valores 254 (veg. densa) y 255 (agua) quedan como NaN.
    """
    resultado = np.full(valor.shape, np.nan, dtype=np.float32)
    mascara_valida = (valor < VALOR_VEG_DENSA) & (valor > 0)
    resultado[mascara_valida] = valor[mascara_valida].astype(np.float32) / 2.54
    return resultado


def fetch_humedad_suelo(
    fecha_inicio: date,
    fecha_fin: date,
    config: SHConfig | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Descarga raster de humedad superficial del suelo (Sentinel-1).

    Returns:
        humedad_pct: ndarray float32 con % humedad (NaN = agua/veg. densa)
        mascara_agua: ndarray bool True donde hay agua libre
    """
    if config is None:
        config = get_sentinelhub_config()

    west, south, east, north = get_rivera_bbox()
    bbox = BBox(bbox=(west, south, east, north), crs=CRS.WGS84)
    size = bbox_to_dimensions(bbox, resolution=RESOLUTION)

    evalscript = EVALSCRIPT_S1.read_text(encoding="utf-8")

    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL1_IW,
                time_interval=(str(fecha_inicio), str(fecha_fin)),
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.TIFF)],
        bbox=bbox,
        size=size,
        config=config,
    )

    data = request.get_data()
    raw = data[0].squeeze()

    humedad_pct = uint8_a_porcentaje(raw)
    mascara_agua = raw == VALOR_AGUA_LIBRE

    logger.info(
        "Humedad S1 descargada: %s — shape=%s, válidos=%d%%, agua=%.1f%%",
        fecha_fin,
        raw.shape,
        int(100 * np.sum(~np.isnan(humedad_pct)) / raw.size),
        100 * np.sum(mascara_agua) / raw.size,
    )

    return humedad_pct, mascara_agua


def calcular_estadisticas_humedad(humedad_pct: np.ndarray) -> dict:
    """Estadísticas de la capa de humedad del suelo."""
    validos = humedad_pct[~np.isnan(humedad_pct)]
    if len(validos) == 0:
        return {"cobertura_pct": 0}
    return {
        "cobertura_pct": round(100 * len(validos) / humedad_pct.size, 1),
        "media": round(float(np.mean(validos)), 2),
        "mediana": round(float(np.median(validos)), 2),
        "p10": round(float(np.percentile(validos, 10)), 2),
        "p25": round(float(np.percentile(validos, 25)), 2),
        "p75": round(float(np.percentile(validos, 75)), 2),
        "p90": round(float(np.percentile(validos, 90)), 2),
    }
