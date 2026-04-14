from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any

import httpx
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.humedad import AOIUnit
from app.models.materialized import SatelliteScene, SceneCoverage
from app.services.catalog import DEPARTMENTS, department_payloads

try:
    from data_fetcher import CATALOG_URL, get_token as legacy_get_token
except Exception:  # pragma: no cover
    CATALOG_URL = None
    legacy_get_token = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _date_to_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _normalized_collection_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "rgb": "sentinel-2-l2a",
        "ndvi": "sentinel-2-l2a",
        "ndmi": "sentinel-2-l2a",
        "ndwi": "sentinel-2-l2a",
        "savi": "sentinel-2-l2a",
        "alerta_fusion": "sentinel-2-l2a",
        "sar": "sentinel-1-grd",
        "lst": "sentinel-3-slstr",
        "s2_l2a": "sentinel-2-l2a",
        "s1_grd": "sentinel-1-grd",
        "s3_slstr": "sentinel-3-slstr",
        "sentinel-2": "sentinel-2-l2a",
        "sentinel-1": "sentinel-1-grd",
        "sentinel-3": "sentinel-3-slstr",
    }
    return aliases.get(normalized, normalized or "sentinel-2-l2a")


def _collection_defaults(collection: str) -> tuple[str | None, str | None]:
    normalized = _normalized_collection_name(collection)
    if normalized == "sentinel-2-l2a":
        return "Sentinel-2", "OPTICAL"
    if normalized == "sentinel-1-grd":
        return "Sentinel-1", "SAR"
    if normalized == "sentinel-3-slstr":
        return "Sentinel-3", "THERMAL"
    return None, None


def _feature_bbox(feature: dict[str, Any]) -> tuple[float, float, float, float] | None:
    bbox = feature.get("bbox")
    if isinstance(bbox, list) and len(bbox) >= 4:
        try:
            return float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
        except Exception:
            return None
    geometry = feature.get("geometry")
    if not geometry:
        return None
    try:
        return tuple(float(item) for item in shape(geometry).bounds)
    except Exception:
        return None


def _department_geometries() -> dict[str, dict[str, Any]]:
    payloads = department_payloads(refresh_geometries=False)
    return {str(item["department"]): item for item in payloads}


@lru_cache(maxsize=1)
def _national_geometry_payload() -> dict[str, Any] | None:
    payloads = department_payloads(refresh_geometries=False)
    geometries = []
    for item in payloads:
        geometry = item.get("geometry_geojson")
        if not geometry:
            continue
        try:
            geometries.append(shape(geometry))
        except Exception:
            continue
    if not geometries:
        return None
    try:
        return mapping(unary_union(geometries))
    except Exception:
        return None


def _geometry_intersection_ratios(
    aoi_geometry: dict[str, Any],
    scene_geometry: dict[str, Any] | None,
    bbox_values: tuple[float, float, float, float] | None,
) -> tuple[float, float, str]:
    """
    Returns:
      - covered_area_pct: % of AOI area covered by the scene geometry (preferred: footprint; fallback: bbox)
      - scene_overlap_pct: % of scene geometry overlapping the AOI (useful to detect edge slivers)
      - geometry_source: "footprint" | "bbox" | "none"

    Notes:
    - We intentionally compute *AOI-relative* coverage here. Cloud/no-data are handled separately.
    - Shapely areas are in degrees^2 for EPSG:4326 GeoJSON; ratios remain meaningful.
    """
    aoi_geom = shape(aoi_geometry)
    aoi_area = max(float(aoi_geom.area), 1e-9)

    scene_geom = None
    geometry_source = "none"
    if scene_geometry:
        try:
            scene_geom = shape(scene_geometry)
            geometry_source = "footprint"
        except Exception:
            scene_geom = None
    if scene_geom is None and bbox_values is not None:
        try:
            scene_geom = box(*bbox_values)
            geometry_source = "bbox"
        except Exception:
            scene_geom = None

    if scene_geom is None:
        return 0.0, 0.0, "none"

    try:
        intersection = aoi_geom.intersection(scene_geom)
    except Exception:
        return 0.0, 0.0, geometry_source
    if intersection.is_empty:
        return 0.0, 0.0, geometry_source

    intersection_area = max(float(intersection.area), 0.0)
    scene_area = max(float(scene_geom.area), 1e-9)

    covered_area_pct = min(100.0, max(0.0, (intersection_area / aoi_area) * 100.0))
    scene_overlap_pct = min(100.0, max(0.0, (intersection_area / scene_area) * 100.0))
    return round(covered_area_pct, 2), round(scene_overlap_pct, 2), geometry_source


async def _department_units(session: AsyncSession) -> dict[str, AOIUnit]:
    result = await session.execute(
        select(AOIUnit).where(
            AOIUnit.unit_type == "department",
            AOIUnit.active.is_(True),
        )
    )
    rows = result.scalars().all()
    return {str(row.department): row for row in rows}


async def _search_catalog_features(
    *,
    token: str,
    collection: str,
    bbox_values: tuple[float, float, float, float],
    start_date: date,
    end_date: date,
    limit: int,
) -> list[dict[str, Any]]:
    if not CATALOG_URL:
        return []
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "collections": [_normalized_collection_name(collection)],
        "bbox": [round(value, 6) for value in bbox_values],
        "datetime": f"{start_date.isoformat()}T00:00:00Z/{end_date.isoformat()}T23:59:59Z",
        "limit": max(1, min(int(limit or 50), 200)),
    }
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.post(CATALOG_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
    return list(data.get("features") or [])


async def sync_scene_catalog(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    departments: list[str] | None = None,
    collections: list[str] | None = None,
    limit_per_request: int = 50,
) -> dict[str, Any]:
    if legacy_get_token is None or CATALOG_URL is None:
        return {
            "status": "disabled",
            "scene_count": 0,
            "coverage_count": 0,
            "departments": departments or [],
            "collections": collections or [],
        }

    token = await asyncio.to_thread(legacy_get_token)
    dept_lookup = _department_geometries()
    dept_units = await _department_units(session)
    target_departments = departments or [record.name for record in DEPARTMENTS]
    target_collections = collections or list(settings.raster_catalog_default_collections or ["sentinel-2-l2a", "sentinel-1-grd", "sentinel-3-slstr"])
    full_country_sync = len({str(item) for item in target_departments}) >= len(DEPARTMENTS)
    national_geometry = _national_geometry_payload() if full_country_sync else None

    scene_count = 0
    coverage_count = 0
    processed_scenes: set[str] = set()

    for department_name in target_departments:
        department_payload = dept_lookup.get(str(department_name))
        if not department_payload:
            continue
        try:
            bbox_values = tuple(float(item) for item in shape(department_payload["geometry_geojson"]).bounds)
        except Exception:
            continue

        for collection in target_collections:
            try:
                features = await _search_catalog_features(
                    token=token,
                    collection=collection,
                    bbox_values=bbox_values,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit_per_request,
                )
            except Exception:
                continue

            for feature in features:
                scene_id = str(
                    feature.get("id")
                    or feature.get("properties", {}).get("id")
                    or feature.get("properties", {}).get("title")
                    or ""
                ).strip()
                if not scene_id:
                    continue
                bbox = _feature_bbox(feature)
                if bbox is None:
                    continue
                properties = feature.get("properties") or {}
                acquired_raw = (
                    properties.get("datetime")
                    or properties.get("start_datetime")
                    or properties.get("updated")
                    or properties.get("created")
                )
                try:
                    acquired_at = datetime.fromisoformat(str(acquired_raw).replace("Z", "+00:00"))
                except Exception:
                    acquired_at = _date_to_datetime(start_date)
                if acquired_at.tzinfo is None:
                    acquired_at = acquired_at.replace(tzinfo=timezone.utc)
                platform_default, orbit_default = _collection_defaults(collection)
                row = await session.get(SatelliteScene, scene_id)
                if row is None:
                    row = SatelliteScene(scene_id=scene_id)
                    session.add(row)
                row.provider = "Copernicus"
                row.collection = _normalized_collection_name(collection)
                row.platform = str(properties.get("platform") or platform_default or "")
                row.acquired_at = acquired_at
                row.footprint_geojson = feature.get("geometry")
                row.bbox = list(bbox)
                row.epsg = int(properties.get("proj:epsg") or 4326)
                row.tile_id = str(properties.get("s2:mgrs_tile") or properties.get("mgrs:tile") or properties.get("sat:relative_orbit") or "")
                row.orbit = str(
                    properties.get("sat:orbit_state")
                    or properties.get("sat:orbit")
                    or properties.get("sat:relative_orbit")
                    or ""
                )
                row.cloud_cover_scene_pct = float(properties.get("eo:cloud_cover") or properties.get("s2:cloud_cover") or 0.0)
                feature_assets = feature.get("assets") or {}
                row.quicklook_url = str(
                    feature_assets.get("thumbnail", {}).get("href")
                    or feature_assets.get("overview", {}).get("href")
                    or feature_assets.get("preview", {}).get("href")
                    or ""
                )
                row.assets_json = feature_assets
                row.raw_metadata = feature
                row.updated_at = _now_utc()
                if scene_id not in processed_scenes:
                    scene_count += 1
                    processed_scenes.add(scene_id)

                for coverage_department in target_departments:
                    department_geometry_payload = dept_lookup.get(str(coverage_department))
                    if not department_geometry_payload:
                        continue
                    geometry = department_geometry_payload["geometry_geojson"]
                    try:
                        covered_area_pct, scene_overlap_pct, geometry_source = _geometry_intersection_ratios(
                            geometry,
                            row.footprint_geojson,
                            bbox,
                        )
                    except Exception:
                        continue
                    if covered_area_pct <= 0.0:
                        continue
                    department_unit = dept_units.get(str(coverage_department))
                    coverage_key = f"dept:{coverage_department}:{bbox[0]:.3f}:{bbox[1]:.3f}:{bbox[2]:.3f}:{bbox[3]:.3f}"
                    result = await session.execute(
                        select(SceneCoverage).where(
                            SceneCoverage.scene_id == scene_id,
                            SceneCoverage.scope_type == "departamento",
                            SceneCoverage.scope_ref == str(coverage_department),
                            SceneCoverage.bbox_bucket == coverage_key,
                        ).limit(1)
                    )
                    coverage_row = result.scalar_one_or_none()
                    if coverage_row is None:
                        coverage_row = SceneCoverage(
                            scene_id=scene_id,
                            scope_type="departamento",
                            scope_ref=str(coverage_department),
                            bbox_bucket=coverage_key,
                        )
                        session.add(coverage_row)
                        coverage_count += 1
                    coverage_row.unit_id = department_unit.id if department_unit else None
                    coverage_row.department = str(coverage_department)

                    # Coverage metrics are AOI-relative. We approximate "valid/cloud" using the scene-level
                    # cloud cover, scaled by the AOI area that is actually covered by the scene geometry.
                    scene_cloud_pct = max(0.0, min(float(row.cloud_cover_scene_pct or 0.0), 100.0))
                    cloud_aoi_pct = round(min(covered_area_pct, covered_area_pct * (scene_cloud_pct / 100.0)), 2)
                    valid_aoi_pct = round(max(0.0, min(covered_area_pct, covered_area_pct - cloud_aoi_pct)), 2)
                    nodata_aoi_pct = round(max(0.0, 100.0 - covered_area_pct), 2)
                    renderable_aoi_pct = valid_aoi_pct

                    coverage_row.covered_area_pct = covered_area_pct
                    coverage_row.valid_pixel_pct = valid_aoi_pct
                    coverage_row.cloud_pixel_pct = cloud_aoi_pct
                    coverage_row.nodata_pixel_pct = nodata_aoi_pct
                    coverage_row.renderable_pixel_pct = renderable_aoi_pct
                    coverage_row.visual_empty = 1 if renderable_aoi_pct < 5.0 else 0
                    coverage_row.quality_score = round(renderable_aoi_pct, 2)
                    coverage_row.rank_within_day = 1
                    coverage_row.metadata_extra = {
                        "collection": row.collection,
                        "provider": row.provider,
                        "scene_bbox": list(bbox),
                        "coverage_geometry_source": geometry_source,
                        "scene_overlap_pct": scene_overlap_pct,
                        "scene_cloud_cover_scene_pct": scene_cloud_pct,
                        "acquired_at": row.acquired_at.isoformat() if row.acquired_at else None,
                        "platform": row.platform,
                        "orbit": row.orbit,
                        "sensor_mode": orbit_default,
                        "tile_id": row.tile_id,
                        "coverage_bucket": f"dept:{coverage_department}",
                    }
                    coverage_row.updated_at = _now_utc()

                if national_geometry is not None:
                    try:
                        covered_area_pct, scene_overlap_pct, geometry_source = _geometry_intersection_ratios(
                            national_geometry,
                            row.footprint_geojson,
                            bbox,
                        )
                    except Exception:
                        covered_area_pct, scene_overlap_pct, geometry_source = 0.0, 0.0, "none"
                    if covered_area_pct > 0.0:
                        scene_cloud_pct = max(0.0, min(float(row.cloud_cover_scene_pct or 0.0), 100.0))
                        cloud_aoi_pct = round(min(covered_area_pct, covered_area_pct * (scene_cloud_pct / 100.0)), 2)
                        valid_aoi_pct = round(max(0.0, min(covered_area_pct, covered_area_pct - cloud_aoi_pct)), 2)
                        nodata_aoi_pct = round(max(0.0, 100.0 - covered_area_pct), 2)
                        renderable_pct = valid_aoi_pct
                        coverage_key = f"national:{bbox[0]:.3f}:{bbox[1]:.3f}:{bbox[2]:.3f}:{bbox[3]:.3f}"
                        result = await session.execute(
                            select(SceneCoverage).where(
                                SceneCoverage.scene_id == scene_id,
                                SceneCoverage.scope_type == "nacional",
                                SceneCoverage.scope_ref == "Uruguay",
                                SceneCoverage.bbox_bucket == coverage_key,
                            ).limit(1)
                        )
                        national_row = result.scalar_one_or_none()
                        if national_row is None:
                            national_row = SceneCoverage(
                                scene_id=scene_id,
                                scope_type="nacional",
                                scope_ref="Uruguay",
                                bbox_bucket=coverage_key,
                            )
                            session.add(national_row)
                            coverage_count += 1
                        national_row.unit_id = None
                        national_row.department = "Uruguay"
                        national_row.covered_area_pct = covered_area_pct
                        national_row.valid_pixel_pct = valid_aoi_pct
                        national_row.cloud_pixel_pct = cloud_aoi_pct
                        national_row.nodata_pixel_pct = nodata_aoi_pct
                        national_row.renderable_pixel_pct = renderable_pct
                        national_row.visual_empty = 1 if renderable_pct < 5.0 else 0
                        national_row.quality_score = round(renderable_pct, 2)
                        national_row.rank_within_day = 1
                        national_row.metadata_extra = {
                            "collection": row.collection,
                            "provider": row.provider,
                            "scene_bbox": list(bbox),
                            "coverage_geometry_source": geometry_source,
                            "scene_overlap_pct": scene_overlap_pct,
                            "scene_cloud_cover_scene_pct": scene_cloud_pct,
                            "acquired_at": row.acquired_at.isoformat() if row.acquired_at else None,
                            "platform": row.platform,
                            "orbit": row.orbit,
                            "sensor_mode": orbit_default,
                            "tile_id": row.tile_id,
                            "coverage_bucket": "national:Uruguay",
                        }
                        national_row.updated_at = _now_utc()

    await session.flush()
    return {
        "status": "success",
        "scene_count": scene_count,
        "coverage_count": coverage_count,
        "date_from": start_date.isoformat(),
        "date_to": end_date.isoformat(),
        "departments": target_departments,
        "collections": [_normalized_collection_name(item) for item in target_collections],
        "national_coverage": full_country_sync,
    }


async def scene_catalog_status(
    session: AsyncSession,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    scene_query = select(SatelliteScene)
    if start_date is not None:
        scene_query = scene_query.where(SatelliteScene.acquired_at >= _date_to_datetime(start_date))
    if end_date is not None:
        scene_query = scene_query.where(SatelliteScene.acquired_at < _date_to_datetime(end_date + timedelta(days=1)))
    scene_result = await session.execute(scene_query)
    scenes = scene_result.scalars().all()

    coverage_query = select(SceneCoverage)
    coverage_result = await session.execute(coverage_query)
    coverages = coverage_result.scalars().all()

    by_collection: dict[str, int] = {}
    for row in scenes:
        by_collection[row.collection] = by_collection.get(row.collection, 0) + 1

    by_department: dict[str, int] = {}
    for row in coverages:
        key = str(row.department or row.scope_ref or "unknown")
        by_department[key] = by_department.get(key, 0) + 1

    return {
        "scene_count": len(scenes),
        "coverage_count": len(coverages),
        "collections": by_collection,
        "departments": by_department,
        "date_from": start_date.isoformat() if start_date else None,
        "date_to": end_date.isoformat() if end_date else None,
    }
