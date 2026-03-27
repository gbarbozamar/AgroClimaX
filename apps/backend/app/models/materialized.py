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
    calibration_ref = Column(String(64))
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
