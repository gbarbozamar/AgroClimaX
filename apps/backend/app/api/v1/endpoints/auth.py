from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.auth import (
    build_google_login_redirect,
    current_user_payload,
    logout_auth_session,
    require_authenticated_request,
    resolve_google_callback,
)


router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/google/login")
async def auth_google_login(
    request: Request,
    next: str | None = Query("/", alias="next"),
):
    return await build_google_login_redirect(request, next_path=next)


@router.get("/google/callback", name="auth_google_callback")
async def auth_google_callback(
    request: Request,
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await resolve_google_callback(request, db, code=code, state=state, error=error)


@router.get("/me")
async def auth_me(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await current_user_payload(request, db)


@router.post("/logout")
async def auth_logout(
    request: Request,
    _: object = Depends(require_authenticated_request),
    db: AsyncSession = Depends(get_db),
):
    return await logout_auth_session(request, db)
