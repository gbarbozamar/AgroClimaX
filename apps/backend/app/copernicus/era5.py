"""
Ingesta de datos ERA5 desde Copernicus Climate Data Store (CDS).
Variables: precipitación, temperatura, ETP para Rivera, Uruguay.
Calcula SPI (Índice de Precipitación Estandarizado).
"""
import logging
import tempfile
from datetime import date
from pathlib import Path

import cdsapi
import numpy as np
import xarray as xr
from scipy.stats import norm

from app.core.config import settings
from app.copernicus.aoi import get_rivera_bbox

logger = logging.getLogger(__name__)


def _get_cds_client() -> cdsapi.Client:
    return cdsapi.Client(
        url=settings.cds_api_url,
        key=settings.cds_api_key,
        quiet=True,
    )


def fetch_era5_mensual(anio: int, mes: int) -> xr.Dataset:
    """
    Descarga ERA5 mensual para Rivera:
    - total_precipitation
    - 2m_temperature
    - potential_evaporation

    Returns:
        xarray Dataset con variables mensuales.
    """
    west, south, east, north = get_rivera_bbox()
    area = [north, west, south, east]  # CDS: N/W/S/E

    c = _get_cds_client()

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp_path = tmp.name

    c.retrieve(
        "reanalysis-era5-single-levels-monthly-means",
        {
            "product_type": "monthly_averaged_reanalysis",
            "variable": [
                "total_precipitation",
                "2m_temperature",
                "potential_evaporation",
            ],
            "year": str(anio),
            "month": str(mes).zfill(2),
            "time": "00:00",
            "area": area,
            "format": "netcdf",
        },
        tmp_path,
    )

    ds = xr.open_dataset(tmp_path)
    logger.info("ERA5 descargado: %d-%02d", anio, mes)
    return ds


def calcular_spi(series_precip_mm: np.ndarray, escala: int = 30) -> np.ndarray:
    """
    Calcula SPI (Índice de Precipitación Estandarizado).

    Args:
        series_precip_mm: Serie temporal de precipitación en mm (tiempo, )
        escala: Ventana en días (30=SPI-1, 90=SPI-3)

    Returns:
        Array SPI de la misma longitud. NaN donde no hay suficientes datos.
    """
    n = len(series_precip_mm)
    spi = np.full(n, np.nan)

    if n < escala:
        return spi

    for i in range(escala - 1, n):
        ventana = series_precip_mm[i - escala + 1 : i + 1]
        if np.any(np.isnan(ventana)):
            continue
        # Ajuste gamma → transformación normal (simplificación empírica)
        media = np.mean(ventana)
        std = np.std(ventana)
        if std == 0:
            spi[i] = 0.0
        else:
            spi[i] = (ventana[-1] - media) / std

    return spi


def clasificar_spi(valor_spi: float) -> str:
    """Clasifica categoría según valor SPI."""
    if np.isnan(valor_spi):
        return "sin_datos"
    if valor_spi >= 2.0:
        return "extremadamente_humedo"
    if valor_spi >= 1.5:
        return "muy_humedo"
    if valor_spi >= 1.0:
        return "moderadamente_humedo"
    if valor_spi > -1.0:
        return "normal"
    if valor_spi > -1.5:
        return "moderadamente_seco"
    if valor_spi > -2.0:
        return "severamente_seco"
    return "extremadamente_seco"
