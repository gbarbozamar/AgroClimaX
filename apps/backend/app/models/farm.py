from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String, Text

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class FarmEstablishment(Base):
    __tablename__ = "farm_establishments"

    id = Column(String(36), primary_key=True, default=new_uuid)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_farm_establishments_user_name", "user_id", "name"),
    )


class FarmField(Base):
    __tablename__ = "farm_fields"

    id = Column(String(36), primary_key=True, default=new_uuid)
    establishment_id = Column(String(36), ForeignKey("farm_establishments.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    department = Column(String(120), nullable=False, index=True)
    padron_value = Column(String(64), nullable=False, index=True)
    padron_source = Column(String(64), nullable=False, default="snig_padronario_rural")
    padron_lookup_payload = Column(JSON, default=dict)
    padron_geometry_geojson = Column(JSON)
    field_geometry_geojson = Column(JSON, nullable=False)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    area_ha = Column(Float)
    aoi_unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_farm_fields_user_establishment", "user_id", "establishment_id"),
        Index("ix_farm_fields_user_padron", "user_id", "department", "padron_value"),
    )


class FarmPaddock(Base):
    __tablename__ = "farm_paddocks"

    id = Column(String(36), primary_key=True, default=new_uuid)
    field_id = Column(String(36), ForeignKey("farm_fields.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    geometry_geojson = Column(JSON, nullable=False)
    area_ha = Column(Float)
    aoi_unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=True, index=True)
    display_order = Column(Integer, nullable=False, default=0)
    active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_farm_paddocks_user_field", "user_id", "field_id"),
        Index("ix_farm_paddocks_field_name", "field_id", "name"),
    )


class PadronLookupCache(Base):
    __tablename__ = "padron_lookup_cache"

    id = Column(String(36), primary_key=True, default=new_uuid)
    department = Column(String(120), nullable=False, index=True)
    padron_value = Column(String(64), nullable=False, index=True)
    provider = Column(String(64), nullable=False, default="snig_padronario_rural")
    query_key = Column(String(255), nullable=False, unique=True, index=True)
    geometry_geojson = Column(JSON)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    raw_payload = Column(JSON, default=dict)
    last_checked_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
