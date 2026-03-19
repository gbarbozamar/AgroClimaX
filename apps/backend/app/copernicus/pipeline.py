"""
Pipeline principal de ingesta diaria Copernicus.
Orquesta: Sentinel-1 → Sentinel-2 → ERA5 → almacenamiento DB.
"""
import logging
from datetime import date, timedelta

import numpy as np

from app.copernicus.sentinel1 import fetch_humedad_suelo, calcular_estadisticas_humedad
from app.copernicus.sentinel2 import fetch_ndmi, calcular_ndmi_estadisticas
from app.copernicus.auth import get_sentinelhub_config

logger = logging.getLogger(__name__)


class ResultadoPipeline:
    def __init__(self):
        self.fecha: date | None = None
        self.humedad_s1: np.ndarray | None = None
        self.mascara_agua: np.ndarray | None = None
        self.ndmi_s2: np.ndarray | None = None
        self.stats_s1: dict = {}
        self.stats_s2: dict = {}
        self.errores: list[str] = []

    @property
    def exitoso(self) -> bool:
        return len(self.errores) == 0

    def resumen(self) -> dict:
        return {
            "fecha": str(self.fecha),
            "exitoso": self.exitoso,
            "s1": self.stats_s1,
            "s2": self.stats_s2,
            "errores": self.errores,
        }


def ejecutar_pipeline(
    fecha: date | None = None,
    ventana_dias: int = 6,
) -> ResultadoPipeline:
    """
    Ejecuta el pipeline completo para una fecha dada.

    Args:
        fecha: Fecha objetivo. Por defecto: ayer.
        ventana_dias: Ventana de búsqueda de imágenes (días).

    Returns:
        ResultadoPipeline con arrays y estadísticas.
    """
    if fecha is None:
        fecha = date.today() - timedelta(days=1)

    resultado = ResultadoPipeline()
    resultado.fecha = fecha

    fecha_inicio = fecha - timedelta(days=ventana_dias)
    config = get_sentinelhub_config()

    # ── Sentinel-1: Humedad superficial suelo ───────────────────────
    try:
        logger.info("Iniciando descarga Sentinel-1 (%s → %s)", fecha_inicio, fecha)
        resultado.humedad_s1, resultado.mascara_agua = fetch_humedad_suelo(
            fecha_inicio, fecha, config
        )
        resultado.stats_s1 = calcular_estadisticas_humedad(resultado.humedad_s1)
        logger.info("S1 OK — humedad media: %.1f%%", resultado.stats_s1.get("media", 0))
    except Exception as exc:
        msg = f"Error Sentinel-1: {exc}"
        logger.error(msg)
        resultado.errores.append(msg)

    # ── Sentinel-2: NDMI ─────────────────────────────────────────────
    try:
        logger.info("Iniciando descarga Sentinel-2 (%s → %s)", fecha_inicio, fecha)
        resultado.ndmi_s2 = fetch_ndmi(fecha_inicio, fecha, config)
        resultado.stats_s2 = calcular_ndmi_estadisticas(resultado.ndmi_s2)
        logger.info(
            "S2 OK — NDMI medio: %.3f (cobertura: %s%%)",
            resultado.stats_s2.get("media", 0),
            resultado.stats_s2.get("cobertura_pct", 0),
        )
    except Exception as exc:
        msg = f"Error Sentinel-2: {exc}"
        logger.error(msg)
        resultado.errores.append(msg)

    logger.info("Pipeline completado: %s", resultado.resumen())
    return resultado
