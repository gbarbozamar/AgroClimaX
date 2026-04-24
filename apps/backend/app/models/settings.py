from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Column, DateTime, Index, Integer, String, Text

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class BusinessSettingsProfile(Base):
    __tablename__ = "business_settings_profiles"

    id = Column(String(36), primary_key=True, default=new_uuid)
    scope_type = Column(String(32), nullable=False)
    scope_key = Column(String(64), nullable=False, default="global")
    version = Column(Integer, nullable=False, default=1)
    rules_json = Column(JSON, default=dict)
    metadata_extra = Column(JSON, default=dict)
    updated_from = Column(String(120))
    updated_by_label = Column(String(120))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_business_settings_scope_key", "scope_type", "scope_key", unique=True),
    )


class BusinessSettingsAudit(Base):
    __tablename__ = "business_settings_audit"

    id = Column(String(36), primary_key=True, default=new_uuid)
    scope_type = Column(String(32), nullable=False, index=True)
    scope_key = Column(String(64), nullable=False, default="global", index=True)
    action = Column(String(32), nullable=False, default="update")
    version_before = Column(Integer, nullable=True)
    version_after = Column(Integer, nullable=True)
    previous_rules_json = Column(JSON, default=dict)
    new_rules_json = Column(JSON, default=dict)
    updated_from = Column(String(120))
    updated_by_label = Column(String(120))
    request_ip = Column(String(120))
    user_agent = Column(Text)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
