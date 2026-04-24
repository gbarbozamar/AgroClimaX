from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class AOIUnit(Base):
    __tablename__ = "aoi_units"

    id = Column(String(64), primary_key=True)
    slug = Column(String(64), unique=True, nullable=False, index=True)
    unit_type = Column(String(32), nullable=False, index=True)
    scope = Column(String(32), nullable=False, default="departamento")
    name = Column(String(120), nullable=False)
    department = Column(String(120), nullable=False, index=True)
    geometry_geojson = Column(JSON)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    coverage_class = Column(String(32), default="pastura_cultivo")
    source = Column(String(64), default="catalog")
    data_mode = Column(String(32), default="simulated")
    metadata_extra = Column(JSON, default=dict)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class SatelliteObservation(Base):
    __tablename__ = "satellite_observations"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    department = Column(String(120), nullable=False, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    coverage_class = Column(String(32), default="pastura_cultivo", index=True)
    vegetation_mask = Column(String(32), default="vegetacion_media", index=True)
    source_mode = Column(String(32), default="simulated")
    s1_vv_db_mean = Column(Float)
    s1_humidity_mean_pct = Column(Float)
    s1_pct_area_stressed = Column(Float)
    s2_ndmi_mean = Column(Float)
    s2_valid_pct = Column(Float)
    cloud_cover_pct = Column(Float)
    lag_hours = Column(Float)
    spi_30d = Column(Float)
    spi_categoria = Column(String(50))
    quality_score = Column(Float, default=0.0)
    quality_control = Column(JSON, default=dict)
    raw_payload = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_satellite_obs_unit_fecha", "unit_id", "observed_at"),
    )


class CalibrationSnapshot(Base):
    __tablename__ = "calibration_snapshots"

    id = Column(String(36), primary_key=True, default=new_uuid)
    department = Column(String(120), nullable=False, index=True)
    coverage_class = Column(String(32), nullable=False, index=True)
    vegetation_mask = Column(String(32), default="vegetacion_media", index=True)
    generated_at = Column(DateTime(timezone=True), nullable=False, index=True)
    window_start = Column(DateTime(timezone=True), nullable=False)
    window_end = Column(DateTime(timezone=True), nullable=False)
    sample_count = Column(Integer, nullable=False, default=0)
    fallback_level = Column(String(64), nullable=False, default="fixed")
    quality_score = Column(Float, nullable=False, default=0.0)
    quantiles = Column(JSON, default=dict)
    coefficients = Column(JSON, default=dict)
    metadata_extra = Column(JSON, default=dict)


class ForecastSignal(Base):
    __tablename__ = "forecast_signals"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    forecast_date = Column(DateTime(timezone=True), nullable=False, index=True)
    precip_mm = Column(Float)
    et0_mm = Column(Float)
    temp_max_c = Column(Float)
    wind_mps = Column(Float)
    spi_trend = Column(Float)
    expected_risk = Column(Float)
    escalation_reason = Column(String(200))
    payload = Column(JSON, default=dict)


class IngestionQualityLog(Base):
    __tablename__ = "ingestion_quality_logs"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    source_mode = Column(String(32), nullable=False, default="simulated")
    provider = Column(String(120), default="copernicus+openmeteo")
    status = Column(String(32), default="success")
    geometry_source = Column(String(120))
    s1_observed_at = Column(DateTime(timezone=True), nullable=True)
    s2_observed_at = Column(DateTime(timezone=True), nullable=True)
    lag_hours = Column(Float)
    valid_coverage_pct = Column(Float)
    cloud_cover_pct = Column(Float)
    quality_score = Column(Float, default=0.0)
    fallback_reason = Column(String(200))
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_ingestion_quality_unit_fecha", "unit_id", "observed_at"),
    )


class GroundTruthMeasurement(Base):
    __tablename__ = "ground_truth_measurements"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    source_type = Column(String(32), nullable=False, index=True)
    sensor_id = Column(String(120))
    soil_moisture_pct = Column(Float)
    pasture_condition = Column(String(64))
    vegetation_condition = Column(String(64))
    confidence = Column(Float, default=50.0)
    notes = Column(String(500))
    geometry_geojson = Column(JSON)
    raw_payload = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class HumedadSuelo(Base):
    """Tabla granular legacy para compatibilidad con reportes previos."""

    __tablename__ = "humedad_suelo"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    fecha = Column(DateTime(timezone=True), nullable=False, index=True)
    geom_geojson = Column(JSON, nullable=True)
    humedad_s1_pct = Column(Float)
    ndmi_s2 = Column(Float)
    nivel_alerta = Column(Integer, nullable=False, default=0)
    fuente_s1 = Column(String(50), default="sentinel-1-grd")
    fuente_s2 = Column(String(50), default="sentinel-2-l2a")
    cobertura_nubes_pct = Column(Float)
    metadata_extra = Column(JSON, default=dict)

    __table_args__ = (
        Index("ix_humedad_fecha_unit", "fecha", "unit_id"),
    )
