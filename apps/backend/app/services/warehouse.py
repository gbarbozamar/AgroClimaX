from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
from typing import Any
from uuid import uuid4

from shapely.geometry import shape
from sqlalchemy import desc, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import SPATIAL_BACKEND_ENABLED
from app.models.humedad import AOIUnit
from app.models.materialized import (
    LatestStateCache,
    SatelliteLayerCatalog,
    SatelliteLayerSnapshot,
    SpatialLayerFeature,
    UnitIndexSnapshot,
)
from app.services.public_api import CAPAS_INFO

try:
    from geoalchemy2.shape import from_shape
except Exception:  # pragma: no cover
    from_shape = None


LAYER_DEFINITIONS = {
    "rgb": {
        "display_name": "RGB Sentinel-2",
        "provider": "Copernicus",
        "source_dataset": "sentinel-2-l2a",
        "description": "Color natural Sentinel-2 para contexto visual.",
        "has_numeric_index": 0,
    },
    "ndvi": {
        "display_name": "NDVI",
        "provider": "Copernicus",
        "source_dataset": "sentinel-2-l2a",
        "description": "Indice de vigor de vegetacion.",
        "has_numeric_index": 0,
    },
    "ndmi": {
        "display_name": "NDMI",
        "provider": "Copernicus",
        "source_dataset": "sentinel-2-l2a",
        "description": "Indice de humedad de vegetacion.",
        "has_numeric_index": 1,
    },
    "ndwi": {
        "display_name": "NDWI",
        "provider": "Copernicus",
        "source_dataset": "sentinel-2-l2a",
        "description": "Indice de agua superficial.",
        "has_numeric_index": 0,
    },
    "savi": {
        "display_name": "SAVI",
        "provider": "Copernicus",
        "source_dataset": "sentinel-2-l2a",
        "description": "Indice de vegetacion ajustado por suelo.",
        "has_numeric_index": 0,
    },
    "sar": {
        "display_name": "SAR VV",
        "provider": "Copernicus",
        "source_dataset": "sentinel-1-grd",
        "description": "Retrodispersión VV asociada a humedad superficial del suelo.",
        "has_numeric_index": 1,
    },
    "alerta_fusion": {
        "display_name": "Fusion de Alerta",
        "provider": "AgroClimaX",
        "source_dataset": "fusion_s1_s2_spi",
        "description": "Capa fusionada de riesgo S1/S2/SPI.",
        "has_numeric_index": 1,
    },
    "lst": {
        "display_name": "Temperatura Superficial",
        "provider": "Copernicus",
        "source_dataset": "sentinel-3-slstr",
        "description": "Temperatura superficial terrestre Sentinel-3.",
        "has_numeric_index": 0,
    },
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def latest_state_cache_key(scope: str, unit_id: str | None = None, department: str | None = None) -> str:
    if scope == "nacional":
        return "state::nacional"
    if unit_id:
        return f"state::{scope}::{unit_id}"
    if department:
        return f"state::{scope}::departamento::{department.lower().replace(' ', '-')}"
    return f"state::{scope}::default"


def _payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(str(payload).encode("utf-8")).hexdigest()


def _safe_observed_at(value: str | None) -> datetime:
    if not value:
        return _now_utc()
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dialect_name(session: AsyncSession) -> str:
    bind = session.get_bind()
    if bind is None:
        return ""
    return bind.dialect.name


def _build_insert(session: AsyncSession, table):
    dialect = _dialect_name(session)
    if dialect == "postgresql":
        return postgresql_insert(table)
    if dialect == "sqlite":
        return sqlite_insert(table)
    return None


async def _fetch_one_by(session: AsyncSession, model, *criteria):
    result = await session.execute(select(model).where(*criteria).limit(1))
    return result.scalar_one_or_none()


def _spatial_geometry_value(geometry_geojson: dict[str, Any] | None):
    if geometry_geojson is None or from_shape is None or not SPATIAL_BACKEND_ENABLED or not settings.database_use_postgis:
        return geometry_geojson
    try:
        return from_shape(shape(geometry_geojson), srid=4326)
    except Exception:
        return geometry_geojson


def _layer_stats_for_key(layer_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("raw_metrics") or {}
    if layer_key == "sar":
        return {
            "humidity_pct": raw.get("s1_humidity_mean_pct"),
            "vv_db_mean": raw.get("s1_vv_db_mean"),
            "affected_pct": payload.get("affected_pct"),
        }
    if layer_key == "ndmi":
        return {
            "ndmi_mean": raw.get("s2_ndmi_mean"),
            "estimated_ndmi": raw.get("estimated_ndmi"),
            "confidence_score": payload.get("confidence_score"),
        }
    if layer_key == "alerta_fusion":
        return {
            "risk_score": payload.get("risk_score"),
            "state": payload.get("state"),
            "affected_pct": payload.get("affected_pct"),
            "largest_cluster_pct": payload.get("largest_cluster_pct"),
        }
    return {
        "state": payload.get("state"),
        "risk_score": payload.get("risk_score"),
        "confidence_score": payload.get("confidence_score"),
    }


async def seed_layer_catalog(session: AsyncSession) -> None:
    for layer_key, info in LAYER_DEFINITIONS.items():
        row = await session.get(SatelliteLayerCatalog, layer_key)
        base_info = CAPAS_INFO.get(layer_key, {})
        metadata = {
            "source_type": base_info.get("src") or base_info.get("fusion"),
            "cloud_filtered": bool(base_info.get("clouds")),
        }
        if row is None:
            session.add(
                SatelliteLayerCatalog(
                    layer_key=layer_key,
                    display_name=info["display_name"],
                    provider=info["provider"],
                    source_dataset=info["source_dataset"],
                    description=info["description"],
                    tile_path_template=f"/api/tiles/{layer_key}/{{z}}/{{x}}/{{y}}.png",
                    has_numeric_index=info["has_numeric_index"],
                    metadata_extra=metadata,
                )
            )
            continue

        row.display_name = info["display_name"]
        row.provider = info["provider"]
        row.source_dataset = info["source_dataset"]
        row.description = info["description"]
        row.tile_path_template = f"/api/tiles/{layer_key}/{{z}}/{{x}}/{{y}}.png"
        row.has_numeric_index = info["has_numeric_index"]
        row.metadata_extra = metadata
    await session.flush()


async def upsert_latest_state_cache(
    session: AsyncSession,
    payload: dict[str, Any],
    *,
    scope: str,
    unit_id: str | None = None,
    department: str | None = None,
) -> LatestStateCache:
    cache_key = latest_state_cache_key(scope, unit_id=unit_id, department=department)
    observed_at = _safe_observed_at(payload.get("observed_at"))
    payload_hash = _payload_hash(payload)
    values = {
        "id": str(uuid4()),
        "cache_key": cache_key,
        "scope": scope,
        "unit_id": unit_id,
        "department": department,
        "observed_at": observed_at,
        "payload": payload,
        "payload_hash": payload_hash,
        "updated_at": datetime.utcnow(),
    }
    insert_stmt = _build_insert(session, LatestStateCache)
    if insert_stmt is not None:
        await session.execute(
            insert_stmt.values(**values).on_conflict_do_update(
                index_elements=[LatestStateCache.cache_key],
                set_={
                    "scope": scope,
                    "unit_id": unit_id,
                    "department": department,
                    "observed_at": observed_at,
                    "payload": payload,
                    "payload_hash": payload_hash,
                    "updated_at": datetime.utcnow(),
                },
            )
        )
        await session.flush()
        row = await _fetch_one_by(session, LatestStateCache, LatestStateCache.cache_key == cache_key)
        if row is not None:
            return row

    row = await _fetch_one_by(session, LatestStateCache, LatestStateCache.cache_key == cache_key)
    if row is None:
        row = LatestStateCache(cache_key=cache_key, scope=scope, unit_id=unit_id, department=department)
        session.add(row)
    row.observed_at = observed_at
    row.payload = payload
    row.payload_hash = payload_hash
    await session.flush()
    return row


async def upsert_index_snapshot(session: AsyncSession, unit: AOIUnit, payload: dict[str, Any]) -> UnitIndexSnapshot:
    observed_at = _safe_observed_at(payload.get("observed_at"))
    raw = payload.get("raw_metrics") or {}
    values = {
        "id": str(uuid4()),
        "unit_id": unit.id,
        "scope": payload.get("scope", unit.scope),
        "department": unit.department,
        "observed_at": observed_at,
        "state": payload.get("state", "Normal"),
        "state_level": payload.get("state_level", 0),
        "risk_score": payload.get("risk_score", 0.0),
        "confidence_score": payload.get("confidence_score", 0.0),
        "affected_pct": payload.get("affected_pct", 0.0),
        "largest_cluster_pct": payload.get("largest_cluster_pct", 0.0),
        "s1_humidity_mean_pct": raw.get("s1_humidity_mean_pct"),
        "s1_vv_db_mean": raw.get("s1_vv_db_mean"),
        "s2_ndmi_mean": raw.get("s2_ndmi_mean"),
        "estimated_ndmi": raw.get("estimated_ndmi"),
        "spi_30d": raw.get("spi_30d"),
        "calibration_ref": payload.get("calibration_ref"),
        "data_mode": payload.get("data_mode", "simulated"),
        "raw_metrics": raw,
        "forecast": payload.get("forecast") or [],
        "drivers": payload.get("drivers") or [],
        "updated_at": datetime.utcnow(),
    }
    insert_stmt = _build_insert(session, UnitIndexSnapshot)
    if insert_stmt is not None:
        await session.execute(
            insert_stmt.values(**values).on_conflict_do_update(
                index_elements=[UnitIndexSnapshot.unit_id, UnitIndexSnapshot.observed_at],
                set_={key: value for key, value in values.items() if key not in {"id", "unit_id", "observed_at"}},
            )
        )
        await session.flush()
        row = await _fetch_one_by(
            session,
            UnitIndexSnapshot,
            UnitIndexSnapshot.unit_id == unit.id,
            UnitIndexSnapshot.observed_at == observed_at,
        )
        if row is not None:
            return row

    row = await _fetch_one_by(
        session,
        UnitIndexSnapshot,
        UnitIndexSnapshot.unit_id == unit.id,
        UnitIndexSnapshot.observed_at == observed_at,
    )
    if row is None:
        row = UnitIndexSnapshot(unit_id=unit.id, scope=payload.get("scope", unit.scope), department=unit.department, observed_at=observed_at)
        session.add(row)
    row.scope = values["scope"]
    row.department = values["department"]
    row.state = values["state"]
    row.state_level = values["state_level"]
    row.risk_score = values["risk_score"]
    row.confidence_score = values["confidence_score"]
    row.affected_pct = values["affected_pct"]
    row.largest_cluster_pct = values["largest_cluster_pct"]
    row.s1_humidity_mean_pct = values["s1_humidity_mean_pct"]
    row.s1_vv_db_mean = values["s1_vv_db_mean"]
    row.s2_ndmi_mean = values["s2_ndmi_mean"]
    row.estimated_ndmi = values["estimated_ndmi"]
    row.spi_30d = values["spi_30d"]
    row.calibration_ref = values["calibration_ref"]
    row.data_mode = values["data_mode"]
    row.raw_metrics = values["raw_metrics"]
    row.forecast = values["forecast"]
    row.drivers = values["drivers"]
    await session.flush()
    return row


async def upsert_layer_snapshots(session: AsyncSession, unit: AOIUnit, payload: dict[str, Any]) -> None:
    observed_at = _safe_observed_at(payload.get("observed_at"))
    availability_score = float(payload.get("confidence_score") or 0.0)
    for layer_key in LAYER_DEFINITIONS:
        summary_stats = _layer_stats_for_key(layer_key, payload)
        metadata_extra = {
            "state": payload.get("state"),
            "provider": LAYER_DEFINITIONS[layer_key]["provider"],
            "observed_at": payload.get("observed_at"),
        }
        values = {
            "id": str(uuid4()),
            "unit_id": unit.id,
            "scope": payload.get("scope", unit.scope),
            "department": unit.department,
            "observed_at": observed_at,
            "layer_key": layer_key,
            "source_mode": payload.get("data_mode", "simulated"),
            "tile_path": f"/api/tiles/{layer_key}/{{z}}/{{x}}/{{y}}.png",
            "availability_score": availability_score,
            "summary_stats": summary_stats,
            "metadata_extra": metadata_extra,
            "updated_at": datetime.utcnow(),
        }
        insert_stmt = _build_insert(session, SatelliteLayerSnapshot)
        if insert_stmt is not None:
            await session.execute(
                insert_stmt.values(**values).on_conflict_do_update(
                    index_elements=[
                        SatelliteLayerSnapshot.unit_id,
                        SatelliteLayerSnapshot.layer_key,
                        SatelliteLayerSnapshot.observed_at,
                    ],
                    set_={key: value for key, value in values.items() if key not in {"id", "unit_id", "layer_key", "observed_at"}},
                )
            )
            continue

        row = await _fetch_one_by(
            session,
            SatelliteLayerSnapshot,
            SatelliteLayerSnapshot.unit_id == unit.id,
            SatelliteLayerSnapshot.layer_key == layer_key,
            SatelliteLayerSnapshot.observed_at == observed_at,
        )
        if row is None:
            row = SatelliteLayerSnapshot(
                unit_id=unit.id,
                scope=payload.get("scope", unit.scope),
                department=unit.department,
                observed_at=observed_at,
                layer_key=layer_key,
            )
            session.add(row)
        row.scope = values["scope"]
        row.department = values["department"]
        row.source_mode = values["source_mode"]
        row.tile_path = values["tile_path"]
        row.availability_score = values["availability_score"]
        row.summary_stats = values["summary_stats"]
        row.metadata_extra = values["metadata_extra"]
    await session.flush()


async def upsert_spatial_layer_feature(session: AsyncSession, unit: AOIUnit, payload: dict[str, Any]) -> SpatialLayerFeature:
    if unit.unit_type == "productive_unit":
        layer_scope = "productiva"
    elif unit.unit_type == "h3_cell":
        layer_scope = "hexagono"
    elif unit.scope == "seccion":
        layer_scope = "seccion"
    else:
        layer_scope = "departamento"
    observed_at = _safe_observed_at(payload.get("observed_at"))
    geometry_geojson = unit.geometry_geojson or payload.get("geometry_geojson") or {"type": "Polygon", "coordinates": []}
    properties = {**payload, "geometry_source": unit.source, "unit_name": unit.name}
    values = {
        "id": str(uuid4()),
        "layer_scope": layer_scope,
        "unit_id": unit.id,
        "department": unit.department,
        "observed_at": observed_at,
        "geometry_geojson": geometry_geojson,
        "geometry": _spatial_geometry_value(geometry_geojson),
        "centroid_lat": unit.centroid_lat,
        "centroid_lon": unit.centroid_lon,
        "properties": properties,
        "updated_at": datetime.utcnow(),
    }
    insert_stmt = _build_insert(session, SpatialLayerFeature)
    if insert_stmt is not None:
        await session.execute(
            insert_stmt.values(**values).on_conflict_do_update(
                index_elements=[SpatialLayerFeature.layer_scope, SpatialLayerFeature.unit_id],
                set_={key: value for key, value in values.items() if key not in {"id", "layer_scope", "unit_id"}},
            )
        )
        await session.flush()
        row = await _fetch_one_by(
            session,
            SpatialLayerFeature,
            SpatialLayerFeature.layer_scope == layer_scope,
            SpatialLayerFeature.unit_id == unit.id,
        )
        if row is not None:
            return row

    row = await _fetch_one_by(
        session,
        SpatialLayerFeature,
        SpatialLayerFeature.layer_scope == layer_scope,
        SpatialLayerFeature.unit_id == unit.id,
    )
    if row is None:
        row = SpatialLayerFeature(layer_scope=layer_scope, unit_id=unit.id, department=unit.department)
        session.add(row)
    row.observed_at = observed_at
    row.department = unit.department
    row.geometry_geojson = geometry_geojson
    row.geometry = values["geometry"]
    row.centroid_lat = unit.centroid_lat
    row.centroid_lon = unit.centroid_lon
    row.properties = properties
    await session.flush()
    return row


async def materialize_unit_payload(
    session: AsyncSession,
    unit: AOIUnit,
    payload: dict[str, Any],
    *,
    update_latest_cache: bool = True,
    update_spatial_features: bool = True,
) -> None:
    if update_latest_cache:
        await upsert_latest_state_cache(session, payload, scope=payload.get("scope", unit.scope), unit_id=unit.id, department=unit.department)
    await upsert_index_snapshot(session, unit, payload)
    await upsert_layer_snapshots(session, unit, payload)
    if update_spatial_features and unit.geometry_geojson:
        await upsert_spatial_layer_feature(session, unit, payload)


async def get_cached_state_payload(
    session: AsyncSession,
    *,
    scope: str,
    unit_id: str | None = None,
    department: str | None = None,
) -> dict[str, Any] | None:
    cache_key = latest_state_cache_key(scope, unit_id=unit_id, department=department)
    result = await session.execute(select(LatestStateCache).where(LatestStateCache.cache_key == cache_key).limit(1))
    row = result.scalar_one_or_none()
    return row.payload if row else None


async def get_cached_layer_features(
    session: AsyncSession,
    *,
    layer_scope: str,
    department: str | None = None,
) -> list[SpatialLayerFeature]:
    query = select(SpatialLayerFeature).where(SpatialLayerFeature.layer_scope == layer_scope).order_by(SpatialLayerFeature.department, SpatialLayerFeature.unit_id)
    if department:
        query = query.where(SpatialLayerFeature.department == department)
    result = await session.execute(query)
    return list(result.scalars().all())


def build_feature_collection(rows: list[SpatialLayerFeature], *, layer_scope: str, department: str | None = None) -> dict[str, Any]:
    observed_dates = [row.observed_at for row in rows if row.observed_at is not None]
    latest_date = max(observed_dates) if observed_dates else None
    cache_status = "current" if is_feature_cache_current(rows) else ("stale" if rows else "missing")
    features = [
        {
            "type": "Feature",
            "geometry": row.geometry_geojson,
            "properties": row.properties or {},
        }
        for row in rows
    ]
    return {
        "type": "FeatureCollection",
        "metadata": {
            "scope": layer_scope,
            "department_filter": department,
            "count": len(features),
            "generated_at": _now_utc().isoformat(),
            "observed_at": latest_date.isoformat() if latest_date else None,
            "source": "database_materialized_cache",
            "cache_status": cache_status,
        },
        "features": features,
    }


def is_feature_cache_current(rows: list[SpatialLayerFeature], target_date: date | None = None) -> bool:
    target_date = target_date or date.today()
    if not rows:
        return False
    return all(row.observed_at and row.observed_at.date() == target_date for row in rows)
