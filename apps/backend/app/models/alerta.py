from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    Column,
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class AlertState(Base):
    __tablename__ = "alert_states"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, unique=True, index=True)
    scope = Column(String(32), nullable=False, default="departamento")
    department = Column(String(120), nullable=False, default="Rivera", index=True)
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    current_state = Column(String(32), nullable=False, index=True)
    state_level = Column(Integer, nullable=False, default=0)
    risk_score = Column(Float, nullable=False, default=0.0)
    confidence_score = Column(Float, nullable=False, default=0.0)
    affected_pct = Column(Float, default=0.0)
    largest_cluster_pct = Column(Float, default=0.0)
    days_in_state = Column(Integer, nullable=False, default=1)
    actionable = Column(Boolean, default=False)
    data_mode = Column(String(32), default="simulated")
    drivers = Column(JSON, default=list)
    forecast = Column(JSON, default=list)
    soil_context = Column(JSON, default=dict)
    calibration_ref = Column(String(255))
    raw_metrics = Column(JSON, default=dict)
    explanation = Column(Text)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class AlertaEvento(Base):
    """Historial de eventos de alerta y tabla de compatibilidad legacy."""

    __tablename__ = "alertas_eventos"

    id = Column(String(36), primary_key=True, default=new_uuid)
    unit_id = Column(String(64), ForeignKey("aoi_units.id"), nullable=False, index=True)
    fecha = Column(DateTime(timezone=True), nullable=False, index=True)
    fecha_fin = Column(DateTime(timezone=True), nullable=True)
    geom_geojson = Column(JSON, nullable=True)
    departamento = Column(String(100), default="Rivera", index=True)
    scope = Column(String(32), default="departamento")
    nivel = Column(Integer, nullable=False, comment="0=Normal 1=Vigilancia 2=Alerta 3=Emergencia")
    nivel_nombre = Column(String(20))
    tipo = Column(String(50), comment="department/custom/live/simulated")
    humedad_media_pct = Column(Float)
    ndmi_medio = Column(Float)
    spi_valor = Column(Float)
    spi_categoria = Column(String(50))
    pct_area_afectada = Column(Float)
    largest_cluster_pct = Column(Float, default=0.0)
    risk_score = Column(Float, default=0.0)
    confidence_score = Column(Float, default=0.0)
    days_in_state = Column(Integer, default=1)
    actionable = Column(Boolean, default=False)
    es_prolongada = Column(Boolean, default=False)
    notificado_email = Column(Boolean, default=False)
    notificado_sms = Column(Boolean, default=False)
    drivers = Column(JSON, default=list)
    forecast = Column(JSON, default=list)
    soil_context = Column(JSON, default=dict)
    calibration_ref = Column(String(255))
    descripcion = Column(Text)
    accion_recomendada = Column(Text)
    metadata_extra = Column(JSON, default=dict)
    creado_en = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_alertas_eventos_unit_fecha", "unit_id", "fecha"),
    )


class NotificationEvent(Base):
    __tablename__ = "notification_events"

    id = Column(String(36), primary_key=True, default=new_uuid)
    alert_event_id = Column(String(36), ForeignKey("alertas_eventos.id"), nullable=True, index=True)
    channel = Column(String(32), nullable=False, index=True)
    recipient = Column(String(200), nullable=False)
    status = Column(String(32), nullable=False, default="queued")
    reason = Column(String(120))
    payload = Column(JSON, default=dict)
    provider_response = Column(JSON, default=dict)
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class AlertSubscription(Base):
    __tablename__ = "alert_subscriptions"

    id = Column(String(36), primary_key=True, default=new_uuid)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    scope_type = Column(String(32), nullable=False, index=True)
    scope_id = Column(String(64), nullable=True, index=True)
    scope_label = Column(String(255), nullable=False)
    channels_json = Column(JSON, default=list)
    min_alert_state = Column(String(32), nullable=False, default="Alerta")
    active = Column(Boolean, default=True, nullable=False, index=True)
    last_sent_state = Column(String(32), nullable=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_alert_subscriptions_user_scope", "user_id", "scope_type", "scope_id"),
    )


class NotificationMediaAsset(Base):
    __tablename__ = "notification_media_assets"

    id = Column(String(36), primary_key=True, default=new_uuid)
    alert_event_id = Column(String(36), ForeignKey("alertas_eventos.id"), nullable=True, index=True)
    subscription_id = Column(String(36), ForeignKey("alert_subscriptions.id"), nullable=True, index=True)
    scope_type = Column(String(32), nullable=False, index=True)
    scope_id = Column(String(64), nullable=True, index=True)
    kind = Column(String(32), nullable=False, index=True)
    mime_type = Column(String(64), nullable=False, default="image/png")
    storage_key = Column(String(255), nullable=False)
    access_token = Column(String(128), nullable=False, unique=True, index=True)
    width = Column(Integer, nullable=False, default=0)
    height = Column(Integer, nullable=False, default=0)
    metadata_extra = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class SuscriptorAlerta(Base):
    __tablename__ = "suscriptores_alertas"

    id = Column(String(36), primary_key=True, default=new_uuid)
    nombre = Column(String(200), nullable=False)
    email = Column(String(200))
    telefono = Column(String(30))
    whatsapp = Column(String(30))
    departamento = Column(String(100), default="Rivera")
    unit_id = Column(String(64), nullable=True)
    nivel_minimo = Column(Integer, default=2)
    activo = Column(Boolean, default=True)
    metadata_extra = Column(JSON, default=dict)
    creado_en = Column(DateTime(timezone=True), default=datetime.utcnow)
