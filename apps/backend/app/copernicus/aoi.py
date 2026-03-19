"""
Área de Interés (AOI) — Departamento de Rivera, Uruguay.

Coordenadas del polígono aproximado de Rivera.
En producción reemplazar por el GeoJSON oficial del IGM Uruguay.
"""
from shapely.geometry import box, mapping
from app.core.config import settings


def get_rivera_bbox():
    """BBox del departamento de Rivera."""
    return (
        settings.aoi_bbox_west,
        settings.aoi_bbox_south,
        settings.aoi_bbox_east,
        settings.aoi_bbox_north,
    )


def get_rivera_geojson() -> dict:
    """GeoJSON de la BBox de Rivera para usar en APIs Copernicus."""
    west, south, east, north = get_rivera_bbox()
    geom = box(west, south, east, north)
    return mapping(geom)


# Puntos de calibración (Díaz, 2026) — Rivera, Uruguay
# Formato: {"id": str, "lon": float, "lat": float, "descripcion": str}
CALIBRATION_POINTS = [
    {"id": "P1", "lon": -54.993322, "lat": -31.489627, "descripcion": "suelo muy seco"},
    {"id": "P2", "lon": -54.988692, "lat": -31.489183, "descripcion": "suelo seco"},
    {"id": "P3", "lon": -54.990746, "lat": -31.500354, "descripcion": "humedad media"},
    {"id": "P4", "lon": -54.975531, "lat": -31.504469, "descripcion": "suelo humedo"},
    {"id": "P5", "lon": -55.004647, "lat": -31.502637, "descripcion": "suelo muy humedo"},
]
