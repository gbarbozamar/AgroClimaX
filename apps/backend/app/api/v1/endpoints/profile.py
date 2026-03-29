from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.auth import AuthContext, require_auth_context
from app.services.profile import get_profile_me, get_profile_schema, save_profile_me


router = APIRouter(prefix="/profile", tags=["profile"])


class ProfileWriteRequest(BaseModel):
    phone_e164: str | None = None
    whatsapp_e164: str | None = None
    organization_name: str | None = None
    organization_type: str | None = None
    role_code: str | None = None
    job_title: str | None = None
    scope_type: str | None = None
    scope_ids_json: list[str] = Field(default_factory=list)
    production_type: str | None = None
    operation_size_hectares: float | None = None
    livestock_headcount: float | None = None
    crop_types_json: list[str] = Field(default_factory=list)
    use_cases_json: list[str] = Field(default_factory=list)
    alert_channels_json: list[str] = Field(default_factory=list)
    min_alert_state: str | None = None
    preferred_language: str | None = None
    communications_opt_in: bool = False
    data_usage_consent: bool = False

    @field_validator("operation_size_hectares", "livestock_headcount", mode="before")
    @classmethod
    def empty_string_to_none(cls, value):
        if value == "":
            return None
        return value

    def to_service_payload(self) -> dict[str, Any]:
        return self.model_dump()


@router.get("/schema")
async def profile_schema(
    db: AsyncSession = Depends(get_db),
):
    return await get_profile_schema(db)


@router.get("/me")
async def profile_me(
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    return await get_profile_me(db, auth.user)


@router.put("/me")
async def profile_save(
    payload: ProfileWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_profile_me(db, auth.user, payload.to_service_payload())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
