from __future__ import annotations

import hashlib
import json
import re
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import shapefile
from shapely.geometry import shape
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alerta import AlertState
from app.models.humedad import AOIUnit
from app.services.analysis import (
    _format_state_payload,
    analyze_unit,
    ensure_latest_daily_analysis,
)
from app.services.catalog import _normalize_department_name, seed_catalog_units
from app.services.warehouse import build_feature_collection, get_cached_layer_features, materialize_unit_payload


def _slugify(value: str) -> str:
    normalized = _normalize_department_name(value)
    slug = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return slug or "unidad"


def _coerce_feature_collection(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("type") != "FeatureCollection":
        raise ValueError("Se esperaba un FeatureCollection GeoJSON")
    features = payload.get("features") or []
    if not isinstance(features, list) or not features:
        raise ValueError("El FeatureCollection no contiene features")
    return features


def _parse_geojson_bytes(content: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(content.decode("utf-8"))
    except Exception as exc:
        raise ValueError("No se pudo leer el GeoJSON cargado") from exc
    _coerce_feature_collection(payload)
    return payload


def _shape_record_properties(reader: shapefile.Reader, record) -> dict[str, Any]:
    if hasattr(record, "as_dict"):
        return record.as_dict()
    field_names = [field[0] for field in reader.fields[1:]]
    return {field_names[index]: value for index, value in enumerate(record)}


def _parse_shapefile_zip_bytes(content: bytes) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="agroclimax_shp_") as tmp_dir:
        temp_path = Path(tmp_dir)
        archive_path = temp_path / "upload.zip"
        archive_path.write_bytes(content)
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(temp_path)

        shp_candidates = sorted(temp_path.rglob("*.shp"))
        if not shp_candidates:
            raise ValueError("El ZIP no contiene ningun .shp")
        shp_path = shp_candidates[0]
        try:
            reader = shapefile.Reader(str(shp_path))
        except Exception as exc:
            raise ValueError("No se pudo leer el shapefile cargado") from exc

        features: list[dict[str, Any]] = []
        try:
            for shape_record in reader.iterShapeRecords():
                geometry = getattr(shape_record.shape, "__geo_interface__", None)
                if not geometry:
                    continue
                properties = _shape_record_properties(reader, shape_record.record)
                features.append(
                    {
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": properties,
                    }
                )
        finally:
            try:
                reader.close()
            except Exception:
                pass

    payload = {"type": "FeatureCollection", "features": features}
    _coerce_feature_collection(payload)
    return payload


def parse_productive_units_file(filename: str, content: bytes) -> dict[str, Any]:
    lower_name = filename.lower()
    if lower_name.endswith(".geojson") or lower_name.endswith(".json"):
        return _parse_geojson_bytes(content)
    if lower_name.endswith(".zip"):
        return _parse_shapefile_zip_bytes(content)
    raise ValueError("Formato no soportado. Use .geojson, .json o .zip con shapefile")


async def _department_geometries(session: AsyncSession) -> list[AOIUnit]:
    await seed_catalog_units(session)
    result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "department"))
    return list(result.scalars().all())


def _infer_department(geom, department_units: list[AOIUnit]) -> str:
    centroid = geom.centroid
    containing: AOIUnit | None = None
    nearest: tuple[float, str] | None = None
    for unit in department_units:
        if not unit.geometry_geojson:
            continue
        department_geom = shape(unit.geometry_geojson)
        if department_geom.contains(centroid) or department_geom.intersects(centroid):
            containing = unit
            break
        distance = department_geom.distance(centroid)
        if nearest is None or distance < nearest[0]:
            nearest = (distance, unit.department)
    if containing is not None:
        return containing.department
    if nearest is not None:
        return nearest[1]
    return "Rivera"


def _feature_name(properties: dict[str, Any], name_field: str | None, category: str, fallback_index: int) -> str:
    if name_field and properties.get(name_field):
        return str(properties.get(name_field)).strip()
    for candidate in ("name", "nombre", "predio", "potrero", "lote", "id"):
        if properties.get(candidate):
            return str(properties.get(candidate)).strip()
    return f"{category.title()} {fallback_index}"


def _feature_external_id(properties: dict[str, Any], external_id_field: str | None) -> str | None:
    if external_id_field and properties.get(external_id_field) is not None:
        return str(properties.get(external_id_field)).strip()
    for candidate in ("id", "ID", "gid", "GID", "objectid", "OBJECTID"):
        if properties.get(candidate) is not None:
            return str(properties.get(candidate)).strip()
    return None


async def import_productive_units(
    session: AsyncSession,
    feature_collection: dict[str, Any],
    *,
    category: str = "predio",
    source_name: str = "user_import",
    name_field: str | None = None,
    external_id_field: str | None = None,
) -> dict[str, Any]:
    features = _coerce_feature_collection(feature_collection)
    department_units = await _department_geometries(session)
    existing_result = await session.execute(select(AOIUnit).where(AOIUnit.unit_type == "productive_unit"))
    existing_units = {unit.id: unit for unit in existing_result.scalars().all()}

    created = 0
    updated = 0
    skipped = 0
    imported_unit_ids: list[str] = []

    for index, feature in enumerate(features, start=1):
        geometry = feature.get("geometry")
        properties = feature.get("properties") or {}
        if not geometry:
            skipped += 1
            continue
        try:
            geom = shape(geometry)
        except Exception:
            skipped += 1
            continue
        if geom.is_empty:
            skipped += 1
            continue
        if geom.geom_type not in {"Polygon", "MultiPolygon"}:
            skipped += 1
            continue

        centroid = geom.centroid
        department = _infer_department(geom, department_units)
        unit_name = _feature_name(properties, name_field, category, index)
        external_id = _feature_external_id(properties, external_id_field)
        geometry_hash = hashlib.sha1(str(geometry).encode("utf-8")).hexdigest()[:12]
        stable_ref = external_id or f"{_slugify(unit_name)}-{geometry_hash}"
        unit_id = f"productive-{_slugify(category)}-{stable_ref}"
        imported_unit_ids.append(unit_id)

        metadata_extra = {
            "unit_category": category,
            "source_name": source_name,
            "external_id": external_id,
            "original_properties": properties,
        }

        existing = existing_units.get(unit_id)
        if existing is None:
            session.add(
                AOIUnit(
                    id=unit_id,
                    slug=unit_id,
                    unit_type="productive_unit",
                    scope="unidad",
                    name=unit_name,
                    department=department,
                    geometry_geojson=geometry,
                    centroid_lat=round(centroid.y, 6),
                    centroid_lon=round(centroid.x, 6),
                    coverage_class="pastura_cultivo",
                    source=source_name,
                    data_mode="derived_department",
                    metadata_extra=metadata_extra,
                )
            )
            created += 1
            continue

        changed = False
        if existing.name != unit_name:
            existing.name = unit_name
            changed = True
        if existing.department != department:
            existing.department = department
            changed = True
        if existing.geometry_geojson != geometry:
            existing.geometry_geojson = geometry
            changed = True
        if existing.centroid_lat != round(centroid.y, 6) or existing.centroid_lon != round(centroid.x, 6):
            existing.centroid_lat = round(centroid.y, 6)
            existing.centroid_lon = round(centroid.x, 6)
            changed = True
        if existing.source != source_name:
            existing.source = source_name
            changed = True
        if existing.metadata_extra != metadata_extra:
            existing.metadata_extra = metadata_extra
            changed = True
        if changed:
            updated += 1

    await session.commit()
    return {
        "status": "success",
        "category": category,
        "source_name": source_name,
        "features_received": len(features),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "unit_ids": imported_unit_ids,
    }


async def import_productive_units_file(
    session: AsyncSession,
    filename: str,
    content: bytes,
    *,
    category: str = "predio",
    source_name: str = "user_import",
    name_field: str | None = None,
    external_id_field: str | None = None,
) -> dict[str, Any]:
    feature_collection = parse_productive_units_file(filename, content)
    result = await import_productive_units(
        session,
        feature_collection,
        category=category,
        source_name=source_name,
        name_field=name_field,
        external_id_field=external_id_field,
    )
    return {
        **result,
        "filename": filename,
        "file_format": "zip_shapefile" if filename.lower().endswith(".zip") else "geojson",
    }


async def materialize_productive_unit_cache(
    session: AsyncSession,
    *,
    target_date: date | None = None,
    department: str | None = None,
    unit_ids: list[str] | None = None,
    ensure_base_analysis: bool = True,
    persist_latest: bool = True,
) -> dict[str, Any]:
    target_date = target_date or date.today()
    if ensure_base_analysis:
        await ensure_latest_daily_analysis(session, target_date=target_date)

    query = select(AOIUnit).where(AOIUnit.unit_type == "productive_unit").order_by(AOIUnit.department, AOIUnit.name)
    if department:
        query = query.where(AOIUnit.department == department)
    if unit_ids:
        query = query.where(AOIUnit.id.in_(unit_ids))
    result = await session.execute(query)
    productive_units = list(result.scalars().all())

    state_result = await session.execute(select(AlertState).order_by(desc(AlertState.observed_at)))
    latest_states = {state.unit_id: state for state in state_result.scalars().all()}
    materialized = 0
    for unit in productive_units:
        metadata_extra = unit.metadata_extra or {}
        if metadata_extra.get("unit_category") == "campo" and metadata_extra.get("source") == "user_field":
            from app.services.farms import materialize_field_analytics_for_unit

            bundle = await materialize_field_analytics_for_unit(session, field_unit=unit, target_date=target_date)
            if bundle is None:
                continue
        else:
            state = latest_states.get(unit.id)
            if state is None or state.observed_at is None or state.observed_at.date() != target_date:
                analysis = await analyze_unit(session, unit=unit, target_date=target_date, geojson=unit.geometry_geojson)
                state = analysis["state"]
            payload = _format_state_payload(unit, state)
            payload = {
                **payload,
                "unit_category": metadata_extra.get("unit_category", "predio"),
                "geometry_source": unit.source,
                "summary_mode": "productive_unit",
            }
            await materialize_unit_payload(
                session,
                unit,
                payload,
                update_latest_cache=persist_latest,
                update_spatial_features=True,
            )
        materialized += 1

    await session.commit()
    return {
        "count": materialized,
        "department_filter": department,
        "unit_filter_count": len(unit_ids or []),
        "observed_at": str(target_date),
    }


async def list_productive_units(session: AsyncSession, department: str | None = None) -> list[dict[str, Any]]:
    rows = await get_cached_layer_features(session, layer_scope="productiva", department=department)
    if not rows:
        await materialize_productive_unit_cache(session, department=department)
        rows = await get_cached_layer_features(session, layer_scope="productiva", department=department)
    return [row.properties or {} for row in rows]


async def productive_units_geojson(session: AsyncSession, department: str | None = None) -> dict[str, Any]:
    rows = await get_cached_layer_features(session, layer_scope="productiva", department=department)
    if not rows:
        await materialize_productive_unit_cache(session, department=department)
        rows = await get_cached_layer_features(session, layer_scope="productiva", department=department)
    collection = build_feature_collection(rows, layer_scope="unidades_productivas", department=department)
    collection["metadata"]["source"] = "database_materialized_cache"
    return collection
