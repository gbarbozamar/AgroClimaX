from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Index, String, Text

from app.db.session import Base


def new_uuid() -> str:
    return str(uuid4())


class AppUser(Base):
    __tablename__ = "app_users"

    id = Column(String(36), primary_key=True, default=new_uuid)
    google_sub = Column(String(128), nullable=False, unique=True, index=True)
    email = Column(String(255), nullable=True, unique=True, index=True)
    email_verified = Column(Boolean, nullable=False, default=False)
    full_name = Column(String(255))
    given_name = Column(String(120))
    family_name = Column(String(120))
    picture_url = Column(Text)
    locale = Column(String(32))
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class AuthSession(Base):
    __tablename__ = "auth_sessions"

    id = Column(String(36), primary_key=True, default=new_uuid)
    user_id = Column(String(36), ForeignKey("app_users.id"), nullable=False, index=True)
    session_token_hash = Column(String(64), nullable=False, unique=True, index=True)
    csrf_token = Column(String(128), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    last_seen_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    ip_hash = Column(String(64))
    user_agent = Column(Text)

    __table_args__ = (
        Index("ix_auth_sessions_user_revoked", "user_id", "revoked_at"),
    )


class AppUserProfile(Base):
    __tablename__ = "app_user_profiles"

    user_id = Column(String(36), ForeignKey("app_users.id"), primary_key=True)
    phone_e164 = Column(String(32))
    whatsapp_e164 = Column(String(32))
    organization_name = Column(String(255))
    organization_type = Column(String(64))
    role_code = Column(String(64))
    job_title = Column(String(120))
    scope_type = Column(String(32))
    scope_ids_json = Column(JSON, default=list)
    production_type = Column(String(64))
    operation_size_hectares = Column(Float)
    livestock_headcount = Column(Float)
    crop_types_json = Column(JSON, default=list)
    use_cases_json = Column(JSON, default=list)
    alert_channels_json = Column(JSON, default=list)
    min_alert_state = Column(String(32))
    preferred_language = Column(String(32))
    communications_opt_in = Column(Boolean, nullable=False, default=False)
    data_usage_consent_at = Column(DateTime(timezone=True), nullable=True)
    questionnaire_version = Column(String(32), nullable=False, default="v1")
    completion_pct = Column(Float, nullable=False, default=0.0)
    profile_completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
