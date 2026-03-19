"""
Ingesta de imágenes Sentinel-2 L2A desde Copernicus Data Space.
Calcula NDMI con filtro de nubes/agua/sombras.
"""
import logging
from datetime import date, timedelta
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

# Resolución en metros
RESOLUTION = 20

# Evalscript NDMI filtrado (desde archivo)
EVALSCRIPT_NDMI = Path(__file__).parent.parent.parent.parent.parent / "evalscripts" / "ndmi_s2_filtrado.js"


def _load_evalscript(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def fetch_ndmi(
    fecha_inicio: date,
    fecha_fin: date,
    config: SHConfig | None = None,
) -> np.ndarray:
    """
    Descarga mosaico NDMI para Rivera en el período dado.

    Returns:
        ndarray shape (H, W) con valores NDMI en [-1, 1].
        NaN donde nubes/agua/sombras.
    """
    if config is None:
        config = get_sentinelhub_config()

    west, south, east, north = get_rivera_bbox()
    bbox = BBox(bbox=(west, south, east, north), crs=CRS.WGS84)
    size = bbox_to_dimensions(bbox, resolution=RESOLUTION)

    evalscript = _load_evalscript(EVALSCRIPT_NDMI)

    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A,
                time_interval=(str(fecha_inicio), str(fecha_fin)),
                mosaicking_order="leastCC",  # menor cobertura de nubes primero
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.TIFF)],
        bbox=bbox,
        size=size,
        config=config,
    )

    data = request.get_data()
    ndmi = data[0].squeeze().astype(np.float32)
    logger.info(
        "NDMI descargado: %s — shape=%s, válidos=%d%%",
        fecha_fin,
        ndmi.shape,
        int(100 * np.sum(~np.isnan(ndmi)) / ndmi.size),
    )
    return ndmi


def calcular_ndmi_estadisticas(ndmi: np.ndarray) -> dict:
    """Estadísticas básicas del array NDMI."""
    validos = ndmi[~np.isnan(ndmi)]
    if len(validos) == 0:
        return {"cobertura_pct": 0}
    return {
        "cobertura_pct": round(100 * len(validos) / ndmi.size, 1),
        "media": round(float(np.mean(validos)), 4),
        "mediana": round(float(np.median(validos)), 4),
        "p10": round(float(np.percentile(validos, 10)), 4),
        "p90": round(float(np.percentile(validos, 90)), 4),
        "min": round(float(np.min(validos)), 4),
        "max": round(float(np.max(validos)), 4),
    }
