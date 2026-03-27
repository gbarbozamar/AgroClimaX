from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.business_settings import (
    clear_coverage_override,
    get_settings_payload,
    get_settings_schema,
    list_settings_audit,
    reset_global_settings,
    save_coverage_override,
    save_global_settings,
)


router = APIRouter(prefix="/settings", tags=["settings"])


class SettingsWriteRequest(BaseModel):
    rules: dict[str, Any] = Field(default_factory=dict)
    operator_label: str | None = None
    updated_from: str | None = "settings_ui"


class SettingsResetRequest(BaseModel):
    operator_label: str | None = None
    updated_from: str | None = "settings_ui"


def _request_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


@router.get("/schema")
async def settings_schema():
    return get_settings_schema()


@router.get("")
async def settings_payload(
    coverage_class: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_settings_payload(db, coverage_class=coverage_class)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/audit")
async def settings_audit(
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_settings_audit(db, limit=limit)
    return {"total": len(rows), "datos": rows}


@router.put("/global")
async def update_global_settings(
    payload: SettingsWriteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_global_settings(
            db,
            payload.rules,
            updated_from=payload.updated_from,
            operator_label=payload.operator_label,
            request_ip=_request_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/overrides/{coverage_class}")
async def update_coverage_settings(
    coverage_class: str,
    payload: SettingsWriteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_coverage_override(
            db,
            coverage_class,
            payload.rules,
            updated_from=payload.updated_from,
            operator_label=payload.operator_label,
            request_ip=_request_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/overrides/{coverage_class}")
async def delete_coverage_override(
    coverage_class: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await clear_coverage_override(
            db,
            coverage_class,
            updated_from="settings_ui",
            operator_label="anonymous",
            request_ip=_request_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/reset/global")
async def reset_global(
    payload: SettingsResetRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await reset_global_settings(
        db,
        updated_from=payload.updated_from,
        operator_label=payload.operator_label,
        request_ip=_request_ip(request),
        user_agent=request.headers.get("user-agent"),
    )


@router.post("/reset/{coverage_class}")
async def reset_coverage_override(
    coverage_class: str,
    payload: SettingsResetRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await clear_coverage_override(
            db,
            coverage_class,
            updated_from=payload.updated_from,
            operator_label=payload.operator_label,
            request_ip=_request_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
