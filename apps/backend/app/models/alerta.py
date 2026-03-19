"""
Modelo: Eventos de alerta hídrica generados por el AlertaEngine.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, Boolean, Text
)
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from app.db.session import Base


class AlertaEvento(Base):
    """Un evento de alerta por fecha y área geográfica afectada."""
    __tablename__ = "alertas_eventos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(DateTime(timezone=True), nullable=False, index=True)
    fecha_fin = Column(DateTime(timezone=True), nullable=True)

    # Área afectada (polígono o punto)
    geom = Column(Geometry("POLYGON", srid=4326), nullable=True,
                  comment="Área geográfica del evento. NULL = departamento completo")
    departamento = Column(String(100), default="Rivera")

    # Clasificación
    nivel = Column(Integer, nullable=False, comment="0=Verde 1=Amarillo 2=Naranja 3=Rojo")
    nivel_nombre = Column(String(20))
    tipo = Column(String(50), comment="hidrico_combinado/solo_s1/solo_s2/spi")

    # Indicadores
    humedad_media_pct = Column(Float)
    ndmi_medio = Column(Float)
    spi_valor = Column(Float)
    spi_categoria = Column(String(50))
    pct_area_afectada = Column(Float)

    # Estado
    es_prolongada = Column(Boolean, default=False)
    notificado_email = Column(Boolean, default=False)
    notificado_sms = Column(Boolean, default=False)

    descripcion = Column(Text)
    accion_recomendada = Column(Text)

    # Metadatos
    creado_en = Column(DateTime(timezone=True), default=datetime.utcnow)
    metadata_extra = Column(JSONB, default=dict)


class SuscriptorAlerta(Base):
    """Usuarios suscritos a notificaciones de alerta."""
    __tablename__ = "suscriptores_alertas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(200), nullable=False)
    email = Column(String(200))
    telefono = Column(String(30))
    departamento = Column(String(100), default="Rivera")

    # Nivel mínimo para notificar
    nivel_minimo = Column(Integer, default=2,
                          comment="Solo notificar si nivel >= este valor (2=Naranja)")

    activo = Column(Boolean, default=True)
    creado_en = Column(DateTime(timezone=True), default=datetime.utcnow)
