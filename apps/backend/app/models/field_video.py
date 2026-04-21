"""Fase 4: FieldVideoJob model.

Guarda jobs de generacion de video temporal por campo/layer/ventana.
Un worker loop los procesa (status=queued -> running -> ready|failed).
"""
from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String, Text

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class FieldVideoJob(Base):
    __tablename__ = "field_video_jobs"

    id = Column(String(36), primary_key=True, default=new_uuid)
    field_id = Column(String(36), ForeignKey("farm_fields.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    layer_key = Column(String(64), nullable=False, index=True)
    duration_days = Column(Integer, nullable=False, default=30)
    status = Column(String(32), nullable=False, default="queued", index=True)
    progress_pct = Column(Float, nullable=False, default=0.0)
    video_path = Column(Text, nullable=True)
    frame_count = Column(Integer, nullable=True)  # nº frames usados al encodear (set al status=ready)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_field_video_jobs_field_layer", "field_id", "layer_key"),
        Index("ix_field_video_jobs_status_created", "status", "created_at"),
    )
