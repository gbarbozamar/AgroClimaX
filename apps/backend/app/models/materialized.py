from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Column, DateTime, Float, ForeignKey, Index, Integer, LargeBinary, String, Text

from app.core.config import settings
from app.db.session import Base, SPATIAL_BACKEND_ENABLED

try:
    from geoalchemy2 import Geometry
except Exception:  # pragma: no cover
    Geometry = None


def new_uuid() -> str:
    return str(uuid4())


def _spatial_column_type():
    if Geometry is not None and SPATIAL_BACKEND_ENABLED and settings.database_use_postgis:
        return Geometry(geometry_type="GEOMETRY", srid=4326, spatial_index=True)
    return JSON


class SatelliteLayerCatalog(Base):
    __tablename__ = "satellite_layer_catalog"

    layer_key = Column(String(32), primary_key=True)
    display_name = Column(String(120), nullable=False)
    provider = Column(String(120), nullable=False, default="Copernicus")
    source_dataset = Column(String(64), nullable=False)
    description = Column(Text)
    tile_path_template = Column(String(240))
    has_numeric_index = Column(Integer, default=0)
    metadata_extra = Column(JSON, default=dict)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class SatelliteScene(Base):
    __tablename__ = "satellite_scenes"

    scene_id = Column(String(180), primary_key=True)
    provider = Column(String(120), nullable=False, index=True, default="Copernicus")
    collection = Column(String(64), nullable=False, index=True)
    platform = Column(String(64), nullable=True, index=True)
    acquired_at = Column(DateTime(timezone=True), nullable=False, index=True)
    footprint_geojson = Column(JSON, nullable=True)
    bbox = Column(JSON, nullable=True)
    epsg = Column(Integer, nullable=True)
    tile_id = Column(String(64), nullable=True, index=True)
    orbit = Column(String(64), nullable=True, index=True)
    cloud_cover_scene_pct = Column(Float, nullable=True)
    quicklook_url = Column(String(500), nullable=True)
    assets_json = Column(JSON, default=dict)
    raw_metadata = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    __table_args__ = (
        # Common access pattern for backfills: filter by collection and time window.
        Index("ix_satellite_scene_collection_acquired_at", "collection", "acquired_at"),
        # Useful for per-tile rebuilds and debugging coverage gaps.
        Index("ix_satellite_scene_tile_acquired_at", "tile_id", "acquired_at"),
    )


class SceneCoverage(Base):
    __tablename__ = "scene_coverages"

    id = Column(String(36), primary_key=True, default=new_uuid)
    scene_id = Column(String(180), ForeignKey("satellite_scenes.scene_id"), nullable=False, index=True)
    scope_type = Column(String(32), nullable=False, index=True)
    scope_ref = Column(String(160), nullable=False, index=True)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    department = Column(String(120), nullable=True, index=True)
    bbox_bucket = Column(String(180), nullable=True, index=True)
    covered_area_pct = Column(Float, nullable=True)
    valid_pixel_pct = Column(Float, nullable=True)
    cloud_pixel_pct = Column(Float, nullable=True)
    nodata_pixel_pct = Column(Float, nullable=True)
    renderable_pixel_pct = Column(Float, nullable=True)
    visual_empty = Column(Integer, nullable=False, default=0, index=True)
    quality_score = Column(Float, nullable=True)
    rank_within_day = Column(Integer, nullable=True)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_scene_coverage_scope", "scene_id", "scope_type", "scope_ref", "bbox_bucket", unique=True),
        Index("ix_scene_coverage_scope_lookup", "scope_type", "scope_ref", "department", "visual_empty"),
        Index("ix_scene_coverage_scope_ref_scene", "scope_type", "scope_ref", "scene_id"),
    )


class SatelliteLayerSnapshot(Base):
    __tablename__ = "satellite_layer_snapshots"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    scope = Column(String(32), nullable=False, index=True)
    department = Column(String(120), nullable=False, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    layer_key = Column(String(32), ForeignKey("satellite_layer_catalog.layer_key"), nullable=False, index=True)
    source_mode = Column(String(32), default="simulated")
    tile_path = Column(String(240))
    availability_score = Column(Float, default=0.0)
    summary_stats = Column(JSON, default=dict)
    metadata_extra = Column(JSON, default=dict)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_layer_snapshot_unit_layer_date", "unit_id", "layer_key", "observed_at", unique=True),
    )


class UnitIndexSnapshot(Base):
    __tablename__ = "unit_index_snapshots"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    scope = Column(String(32), nullable=False, index=True)
    department = Column(String(120), nullable=False, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    state = Column(String(32), nullable=False, default="Normal")
    state_level = Column(Integer, nullable=False, default=0)
    risk_score = Column(Float, default=0.0)
    confidence_score = Column(Float, default=0.0)
    affected_pct = Column(Float, default=0.0)
    largest_cluster_pct = Column(Float, default=0.0)
    s1_humidity_mean_pct = Column(Float)
    s1_vv_db_mean = Column(Float)
    s2_ndmi_mean = Column(Float)
    estimated_ndmi = Column(Float)
    spi_30d = Column(Float)
    calibration_ref = Column(String(255))
    data_mode = Column(String(32), default="simulated")
    raw_metrics = Column(JSON, default=dict)
    forecast = Column(JSON, default=list)
    drivers = Column(JSON, default=list)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_index_snapshot_unit_date", "unit_id", "observed_at", unique=True),
    )


class LatestStateCache(Base):
    __tablename__ = "latest_state_cache"

    id = Column(String(36), primary_key=True, default=new_uuid)
    cache_key = Column(String(160), nullable=False, unique=True, index=True)
    scope = Column(String(32), nullable=False, index=True)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    department = Column(String(120), nullable=True, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    payload = Column(JSON, default=dict)
    payload_hash = Column(String(64))
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class HistoricalStateCache(Base):
    __tablename__ = "historical_state_cache"

    id = Column(String(36), primary_key=True, default=new_uuid)
    cache_key = Column(String(160), nullable=False, index=True)
    scope = Column(String(32), nullable=False, index=True)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    department = Column(String(120), nullable=True, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    payload = Column(JSON, default=dict)
    payload_hash = Column(String(64))
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_historical_state_cache_key_date", "cache_key", "observed_at", unique=True),
    )


class SpatialLayerFeature(Base):
    __tablename__ = "spatial_layer_features"

    id = Column(String(36), primary_key=True, default=new_uuid)
    layer_scope = Column(String(32), nullable=False, index=True)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    department = Column(String(120), nullable=False, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    geometry_geojson = Column(JSON, nullable=False)
    geometry = Column(_spatial_column_type(), nullable=True)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    properties = Column(JSON, default=dict)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_spatial_layer_scope_unit", "layer_scope", "unit_id", unique=True),
        Index("ix_spatial_layer_scope_department", "layer_scope", "department"),
    )


class ExternalMapCacheEntry(Base):
    __tablename__ = "external_map_cache"

    id = Column(String(36), primary_key=True, default=new_uuid)
    cache_key = Column(String(160), nullable=False, unique=True, index=True)
    provider = Column(String(64), nullable=False, index=True, default="coneat")
    request_name = Column(String(32), nullable=False, index=True, default="GETMAP")
    content_type = Column(String(120), nullable=False, default="image/png")
    content = Column(LargeBinary, nullable=False)
    content_hash = Column(String(64))
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class RasterCacheEntry(Base):
    __tablename__ = "raster_cache_entries"

    id = Column(String(36), primary_key=True, default=new_uuid)
    cache_key = Column(String(200), nullable=False, unique=True, index=True)
    layer_id = Column(String(64), nullable=False, index=True)
    cache_kind = Column(String(48), nullable=False, index=True)
    scope_type = Column(String(32), nullable=True, index=True)
    scope_ref = Column(String(160), nullable=True, index=True)
    display_date = Column(DateTime(timezone=True), nullable=True, index=True)
    source_date = Column(DateTime(timezone=True), nullable=True, index=True)
    zoom = Column(Integer, nullable=True, index=True)
    bbox_bucket = Column(String(180), nullable=True, index=True)
    storage_backend = Column(String(32), nullable=False, default="filesystem")
    storage_key = Column(String(255), nullable=True)
    status = Column(String(24), nullable=False, default="missing", index=True)
    bytes_size = Column(Integer, nullable=True)
    metadata_extra = Column(JSON, default=dict)
    last_warmed_at = Column(DateTime(timezone=True), nullable=True, index=True)
    last_hit_at = Column(DateTime(timezone=True), nullable=True, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_raster_cache_lookup", "layer_id", "cache_kind", "display_date", "zoom", "bbox_bucket"),
    )


class RasterProduct(Base):
    __tablename__ = "raster_products"

    id = Column(String(36), primary_key=True, default=new_uuid)
    product_key = Column(String(220), nullable=False, unique=True, index=True)
    layer_id = Column(String(64), nullable=False, index=True)
    product_kind = Column(String(48), nullable=False, default="viewport_bucket_mosaic", index=True)
    scope_type = Column(String(32), nullable=True, index=True)
    scope_ref = Column(String(160), nullable=True, index=True)
    display_date = Column(DateTime(timezone=True), nullable=False, index=True)
    source_date = Column(DateTime(timezone=True), nullable=True, index=True)
    zoom = Column(Integer, nullable=False, index=True)
    bbox_bucket = Column(String(180), nullable=False, index=True)
    storage_backend = Column(String(32), nullable=False, default="filesystem")
    storage_key = Column(String(255), nullable=True)
    content_type = Column(String(120), nullable=False, default="image/png")
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    tile_min_x = Column(Integer, nullable=True)
    tile_min_y = Column(Integer, nullable=True)
    tile_max_x = Column(Integer, nullable=True)
    tile_max_y = Column(Integer, nullable=True)
    visual_empty = Column(Integer, nullable=False, default=0, index=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    bytes_size = Column(Integer, nullable=True)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_raster_product_lookup", "layer_id", "display_date", "zoom", "bbox_bucket", "scope_type", "scope_ref"),
        Index("ix_raster_product_scope_status", "layer_id", "product_kind", "display_date", "scope_ref", "status"),
    )


class RasterMosaic(Base):
    __tablename__ = "raster_mosaics"

    id = Column(String(36), primary_key=True, default=new_uuid)
    mosaic_key = Column(String(220), nullable=False, unique=True, index=True)
    layer_id = Column(String(64), nullable=False, index=True)
    scope_type = Column(String(32), nullable=False, index=True, default="nacional")
    scope_ref = Column(String(160), nullable=False, index=True, default="Uruguay")
    display_date = Column(DateTime(timezone=True), nullable=False, index=True)
    storage_backend = Column(String(32), nullable=False, default="filesystem")
    storage_key = Column(String(255), nullable=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    visual_empty = Column(Integer, nullable=False, default=0, index=True)
    source_product_keys = Column(JSON, default=list)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_raster_mosaic_lookup", "layer_id", "display_date", "scope_type", "scope_ref"),
        Index("ix_raster_mosaic_scope_status", "layer_id", "display_date", "scope_type", "scope_ref", "status"),
    )


class PreloadRun(Base):
    __tablename__ = "preload_runs"

    id = Column(String(36), primary_key=True, default=new_uuid)
    run_key = Column(String(64), nullable=False, unique=True, index=True)
    run_type = Column(String(32), nullable=False, index=True)
    scope_type = Column(String(32), nullable=True, index=True)
    scope_ref = Column(String(160), nullable=True, index=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    progress_total = Column(Integer, nullable=False, default=0)
    progress_done = Column(Integer, nullable=False, default=0)
    stage = Column(String(64), nullable=True, index=True)
    details = Column(JSON, default=dict)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)
