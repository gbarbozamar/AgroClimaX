"""
FieldImageSnapshot — Fase 2 del plan Field Mode.

Cada fila representa un snapshot PNG rendereado por campo × capa × fecha.
El archivo PNG vive en filesystem (o S3) según storage backend; la fila
guarda metadata + referencia storage_key + una copia de metricas del
UnitIndexSnapshot vigente al momento del render, para evitar joins en
runtime del timeline/video.
"""
from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import JSON, Column, Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class FieldImageSnapshot(Base):
    __tablename__ = "field_image_snapshots"

    id = Column(String(36), primary_key=True, default=new_uuid)
    field_id = Column(String(36), ForeignKey("farm_fields.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    layer_key = Column(String(64), nullable=False, index=True)
    observed_at = Column(Date, nullable=False, index=True)

    # Storage pointer: ruta relativa al backend de storage (filesystem o S3 key).
    storage_key = Column(String(512), nullable=False)
    width_px = Column(Integer, nullable=False, default=0)
    height_px = Column(Integer, nullable=False, default=0)
    bbox_json = Column(JSON, nullable=False, default=list)  # [W, S, E, N]
    area_ha = Column(Float, nullable=False, default=0.0)

    # Metadata embebida: copia del UnitIndexSnapshot al momento del render,
    # evita joins en runtime del timeline y permite que el snapshot sea
    # self-contained para el MCP.
    risk_score = Column(Float)
    confidence_score = Column(Float)
    s1_humidity_mean_pct = Column(Float)
    s2_ndmi_mean = Column(Float)
    spi_30d = Column(Float)
    raw_metrics = Column(JSON)

    rendered_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("field_id", "layer_key", "observed_at", name="uq_field_snapshot_unique"),
    )
