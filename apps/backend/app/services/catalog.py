from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any
import unicodedata

import httpx
from shapely.geometry import shape
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.humedad import AOIUnit


BASE_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = BASE_DIR / ".catalog_cache"
BOUNDARIES_CACHE_FILE = CACHE_DIR / "uy_departments_adm1.geojson"


@dataclass(frozen=True)
class DepartmentRecord:
    slug: str
    name: str
    capital: str
    lat: float
    lon: float
    coverage_class: str = "pastura_cultivo"


DEPARTMENTS: tuple[DepartmentRecord, ...] = (
    DepartmentRecord("artigas", "Artigas", "Artigas", -30.4013, -56.4721),
    DepartmentRecord("canelones", "Canelones", "Canelones", -34.5230, -56.2778),
    DepartmentRecord("cerro-largo", "Cerro Largo", "Melo", -32.3703, -54.1675),
    DepartmentRecord("colonia", "Colonia", "Colonia del Sacramento", -34.4714, -57.8442),
    DepartmentRecord("durazno", "Durazno", "Durazno", -33.3806, -56.5236),
    DepartmentRecord("flores", "Flores", "Trinidad", -33.5389, -56.8886),
    DepartmentRecord("florida", "Florida", "Florida", -34.0995, -56.2142),
    DepartmentRecord("lavalleja", "Lavalleja", "Minas", -34.3759, -55.2377),
    DepartmentRecord("maldonado", "Maldonado", "Maldonado", -34.9001, -54.9500),
    DepartmentRecord("montevideo", "Montevideo", "Montevideo", -34.9011, -56.1645, "suelo_desnudo_urbano"),
    DepartmentRecord("paysandu", "Paysandu", "Paysandu", -32.3171, -58.0807),
    DepartmentRecord("rio-negro", "Rio Negro", "Fray Bentos", -33.1325, -58.2956),
    DepartmentRecord("rivera", "Rivera", "Rivera", -30.9053, -55.5508),
    DepartmentRecord("rocha", "Rocha", "Rocha", -34.4833, -54.3333, "humedal"),
    DepartmentRecord("salto", "Salto", "Salto", -31.3833, -57.9667),
    DepartmentRecord("san-jose", "San Jose", "San Jose de Mayo", -34.3375, -56.7136),
    DepartmentRecord("soriano", "Soriano", "Mercedes", -33.2534, -58.0300),
    DepartmentRecord("tacuarembo", "Tacuarembo", "Tacuarembo", -31.7117, -55.9810),
    DepartmentRecord("treinta-y-tres", "Treinta y Tres", "Treinta y Tres", -33.2300, -54.3800),
)


def _normalize_department_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_only.lower().replace("-", " ").replace("_", " ").strip()


def _department_geometry(record: DepartmentRecord) -> dict[str, Any]:
    if record.slug == "rivera":
        return {
            "type": "Polygon",
            "coordinates": [[
                [settings.aoi_bbox_west, settings.aoi_bbox_south],
                [settings.aoi_bbox_east, settings.aoi_bbox_south],
                [settings.aoi_bbox_east, settings.aoi_bbox_north],
                [settings.aoi_bbox_west, settings.aoi_bbox_north],
                [settings.aoi_bbox_west, settings.aoi_bbox_south],
            ]],
        }

    delta_lon = 0.40
    delta_lat = 0.33
    return {
        "type": "Polygon",
        "coordinates": [[
            [record.lon - delta_lon, record.lat - delta_lat],
            [record.lon + delta_lon, record.lat - delta_lat],
            [record.lon + delta_lon, record.lat + delta_lat],
            [record.lon - delta_lon, record.lat + delta_lat],
            [record.lon - delta_lon, record.lat - delta_lat],
        ]],
    }


def _fetch_boundaries_metadata() -> dict[str, Any]:
    if settings.department_boundaries_geojson_url:
        return {"gjDownloadURL": settings.department_boundaries_geojson_url, "boundarySource": "custom_url"}

    response = httpx.get(settings.department_boundaries_metadata_url, timeout=20, follow_redirects=True)
    response.raise_for_status()
    return response.json()


def _download_boundaries_geojson() -> tuple[dict[str, Any], dict[str, Any]]:
    metadata = _fetch_boundaries_metadata()
    geojson_url = metadata.get("gjDownloadURL")
    if not geojson_url:
        raise ValueError("No se pudo resolver la URL GeoJSON de departamentos")

    response = httpx.get(geojson_url, timeout=40, follow_redirects=True)
    response.raise_for_status()
    payload = response.json()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    BOUNDARIES_CACHE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    return payload, metadata


def _load_cached_boundaries() -> dict[str, Any] | None:
    if not BOUNDARIES_CACHE_FILE.exists():
        return None
    try:
        return json.loads(BOUNDARIES_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_department_boundaries(refresh_geometries: bool = False) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    metadata: dict[str, Any] = {"boundarySource": "fallback_boxes", "cache": False}
    collection: dict[str, Any] | None = None

    if refresh_geometries:
        try:
            collection, metadata = _download_boundaries_geojson()
            metadata = {**metadata, "cache": False}
        except Exception:
            collection = _load_cached_boundaries()
            if collection is not None:
                metadata = {"boundarySource": "geoboundaries_cache", "cache": True}
    else:
        collection = _load_cached_boundaries()
        if collection is not None:
            metadata = {"boundarySource": "geoboundaries_cache", "cache": True}

    if not collection:
        return {}, metadata

    features_by_department: dict[str, dict[str, Any]] = {}
    for feature in collection.get("features", []):
        props = feature.get("properties", {})
        department_name = props.get("shapeName")
        geometry = feature.get("geometry")
        if not department_name or not geometry:
            continue
        centroid = shape(geometry).centroid
        features_by_department[_normalize_department_name(department_name)] = {
            "geometry": geometry,
            "centroid_lat": round(centroid.y, 5),
            "centroid_lon": round(centroid.x, 5),
            "shape_iso": props.get("shapeISO"),
            "shape_id": props.get("shapeID"),
            "shape_name": department_name,
        }

    return features_by_department, metadata


def department_payloads(refresh_geometries: bool = False) -> list[dict[str, Any]]:
    boundary_features, boundary_metadata = load_department_boundaries(refresh_geometries=refresh_geometries)
    payloads: list[dict[str, Any]] = []
    for record in DEPARTMENTS:
        normalized_name = _normalize_department_name(record.name)
        boundary = boundary_features.get(normalized_name)
        geometry = boundary["geometry"] if boundary else _department_geometry(record)
        centroid_lat = boundary["centroid_lat"] if boundary else record.lat
        centroid_lon = boundary["centroid_lon"] if boundary else record.lon
        geometry_source = boundary_metadata.get("boundarySource", "fallback_boxes") if boundary else "fallback_boxes"
        payloads.append(
            {
                "id": f"department-{record.slug}",
                "slug": f"departamento-{record.slug}",
                "name": record.name,
                "capital": record.capital,
                "department": record.name,
                "centroid_lat": centroid_lat,
                "centroid_lon": centroid_lon,
                "coverage_class": record.coverage_class,
                "geometry_geojson": geometry,
                "geometry_source": geometry_source,
                "metadata_extra": {
                    "capital": record.capital,
                    "boundary_source": geometry_source,
                    "boundary_cache": boundary_metadata.get("cache", False),
                    "shape_iso": boundary.get("shape_iso") if boundary else None,
                    "shape_id": boundary.get("shape_id") if boundary else None,
                },
            }
        )
    return payloads


async def seed_catalog_units(session: AsyncSession, refresh_geometries: bool = False) -> int:
    existing_result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "department"))
    existing_units = {unit.id: unit for unit in existing_result.scalars().all()}
    created = 0
    updated = 0

    for payload in department_payloads(refresh_geometries=refresh_geometries):
        existing = existing_units.get(payload["id"])
        if existing is None:
            session.add(
                AOIUnit(
                    id=payload["id"],
                    slug=payload["slug"],
                    unit_type="department",
                    scope="departamento",
                    name=payload["name"],
                    department=payload["department"],
                    geometry_geojson=payload["geometry_geojson"],
                    centroid_lat=payload["centroid_lat"],
                    centroid_lon=payload["centroid_lon"],
                    coverage_class=payload["coverage_class"],
                    source=payload["geometry_source"],
                    data_mode="catalog",
                    metadata_extra=payload["metadata_extra"],
                )
            )
            created += 1
            continue

        merged_metadata = {**(existing.metadata_extra or {}), **payload["metadata_extra"]}
        changed = False
        if existing.geometry_geojson != payload["geometry_geojson"]:
            existing.geometry_geojson = payload["geometry_geojson"]
            changed = True
        if existing.centroid_lat != payload["centroid_lat"] or existing.centroid_lon != payload["centroid_lon"]:
            existing.centroid_lat = payload["centroid_lat"]
            existing.centroid_lon = payload["centroid_lon"]
            changed = True
        if existing.coverage_class != payload["coverage_class"]:
            existing.coverage_class = payload["coverage_class"]
            changed = True
        if existing.source != payload["geometry_source"]:
            existing.source = payload["geometry_source"]
            changed = True
        if existing.metadata_extra != merged_metadata:
            existing.metadata_extra = merged_metadata
            changed = True
        if changed:
            updated += 1

    if created or updated:
        await session.commit()

    return created
