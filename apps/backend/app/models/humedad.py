"""
Modelo: Serie temporal de humedad del suelo y NDMI por pixel/grilla.
Tabla optimizada para TimescaleDB (hypertable en columna 'fecha').
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, Boolean, Index, text
)
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from app.db.session import Base


class HumedadSuelo(Base):
    """
    Una fila por (fecha, punto_grilla).
    Resolución de entrega: grilla 100m x 100m (agregación de píxeles 20m).
    """
    __tablename__ = "humedad_suelo"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(DateTime(timezone=True), nullable=False, index=True)

    # Geometría punto centroide de la celda de grilla (EPSG:4326)
    geom = Column(Geometry("POINT", srid=4326), nullable=False)

    # Índices satelitales
    humedad_s1_pct = Column(Float, comment="% humedad superficial Sentinel-1 (~5cm)")
    ndmi_s2 = Column(Float, comment="NDMI Sentinel-2 [-1, 1]")

    # Clasificación
    nivel_alerta = Column(Integer, nullable=False, default=0,
                          comment="0=Verde,1=Amarillo,2=Naranja,3=Rojo")

    # Metadatos fuente
    fuente_s1 = Column(String(50), default="sentinel-1-grd")
    fuente_s2 = Column(String(50), default="sentinel-2-l2a")
    cobertura_nubes_pct = Column(Float, comment="% cobertura nubes S2")

    __table_args__ = (
        Index("ix_humedad_fecha_geom", "fecha", "geom"),
    )
