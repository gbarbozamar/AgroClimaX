from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, Date, DateTime, Float, Index, JSON, String, Text

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(String(36), primary_key=True, default=new_uuid)
    job_key = Column(String(200), nullable=False, unique=True, index=True)
    job_type = Column(String(64), nullable=False, index=True)
    trigger_source = Column(String(32), nullable=False, default="manual", index=True)
    scope = Column(String(64), nullable=False, default="nacional", index=True)
    department = Column(String(120), nullable=True, index=True)
    target_date = Column(Date, nullable=False, index=True)
    scheduled_for = Column(DateTime(timezone=True), nullable=True, index=True)
    status = Column(String(32), nullable=False, default="queued", index=True)
    started_at = Column(DateTime(timezone=True), nullable=True, index=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    details = Column(JSON, default=dict)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_pipeline_runs_job_target", "job_type", "target_date"),
        Index("ix_pipeline_runs_scope_target", "scope", "target_date"),
    )
