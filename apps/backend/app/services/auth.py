from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import Depends, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import get_db
from app.models.auth import AppUser, AuthSession
from app.services.profile import get_profile_status


SAFE_HTTP_METHODS = {"GET", "HEAD", "OPTIONS"}
_DISCOVERY_CACHE: dict[str, Any] | None = None
GOOGLE_HTTP_TIMEOUT_SECONDS = 8


@dataclass(slots=True)
class AuthContext:
    user: AppUser
    session: AuthSession | None
    csrf_token: str
    synthetic: bool = False


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_testing_bypass_enabled() -> bool:
    return settings.app_env == "testing" and settings.auth_bypass_for_tests


def _auth_cookie_secure() -> bool:
    return settings.app_env == "production"


def _sanitize_next_path(value: str | None) -> str:
    candidate = (value or settings.auth_login_success_redirect or "/").strip()
    if not candidate.startswith("/") or candidate.startswith("//"):
        return settings.auth_login_success_redirect or "/"
    return candidate


def _code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("utf-8")


def _callback_url(request: Request) -> str:
    if settings.google_redirect_uri:
        return settings.google_redirect_uri
    return f"{str(request.base_url).rstrip('/')}{settings.api_prefix}/auth/google/callback"


def _serialize_user(user: AppUser) -> dict[str, Any]:
    return {
        "id": user.id,
        "email": user.email,
        "email_verified": bool(user.email_verified),
        "full_name": user.full_name,
        "given_name": user.given_name,
        "family_name": user.family_name,
        "picture_url": user.picture_url,
        "locale": user.locale,
        "is_active": bool(user.is_active),
        "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
    }


def actor_label_from_context(auth: AuthContext | None) -> str:
    if auth is None:
        return "anonymous"
    return auth.user.full_name or auth.user.email or "authenticated-user"


def _testing_auth_context() -> AuthContext:
    now = _now_utc()
    user = AppUser(
        id="test-user",
        google_sub="test-google-sub",
        email="test@agroclimax.local",
        email_verified=True,
        full_name="Test User",
        given_name="Test",
        family_name="User",
        locale="es-UY",
        is_active=True,
        last_login_at=now,
    )
    session = AuthSession(
        id="test-session",
        user_id=user.id,
        session_token_hash="testing",
        csrf_token="testing-csrf-token",
        expires_at=now + timedelta(hours=1),
        last_seen_at=now,
    )
    return AuthContext(user=user, session=session, csrf_token=session.csrf_token, synthetic=True)


async def get_google_discovery_document() -> dict[str, Any]:
    global _DISCOVERY_CACHE
    if _DISCOVERY_CACHE is not None:
        return _DISCOVERY_CACHE
    async with httpx.AsyncClient(timeout=GOOGLE_HTTP_TIMEOUT_SECONDS) as client:
        response = await client.get(settings.google_discovery_url)
        response.raise_for_status()
    _DISCOVERY_CACHE = response.json()
    return _DISCOVERY_CACHE


async def build_google_login_redirect(request: Request, next_path: str | None = None) -> RedirectResponse:
    if not settings.google_oauth_enabled:
        return RedirectResponse(url=f"{settings.auth_login_success_redirect}?auth_error=google_not_configured", status_code=302)

    try:
        discovery = await get_google_discovery_document()
    except Exception:
        return RedirectResponse(
            url=f"{settings.auth_login_success_redirect}?auth_error=google_discovery_unavailable",
            status_code=302,
        )
    state = secrets.token_urlsafe(24)
    verifier = secrets.token_urlsafe(48)
    request.session["google_oauth"] = {
        "state": state,
        "code_verifier": verifier,
        "next": _sanitize_next_path(next_path),
        "created_at": _now_utc().isoformat(),
    }
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": _callback_url(request),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "code_challenge": _code_challenge(verifier),
        "code_challenge_method": "S256",
        "prompt": "select_account",
    }
    return RedirectResponse(url=f"{discovery['authorization_endpoint']}?{urlencode(params)}", status_code=302)


async def _exchange_google_code(request: Request, code: str, verifier: str) -> dict[str, Any]:
    discovery = await get_google_discovery_document()
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "redirect_uri": _callback_url(request),
        "code_verifier": verifier,
    }
    async with httpx.AsyncClient(timeout=GOOGLE_HTTP_TIMEOUT_SECONDS) as client:
        response = await client.post(discovery["token_endpoint"], data=payload)
        response.raise_for_status()
    return response.json()


async def _fetch_google_userinfo(access_token: str) -> dict[str, Any]:
    discovery = await get_google_discovery_document()
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=GOOGLE_HTTP_TIMEOUT_SECONDS) as client:
        response = await client.get(discovery["userinfo_endpoint"], headers=headers)
        response.raise_for_status()
    payload = response.json()
    if not payload.get("sub"):
        raise HTTPException(status_code=400, detail="No se pudo validar el perfil de Google")
    return payload


async def _upsert_google_user(db: AsyncSession, profile: dict[str, Any]) -> AppUser:
    result = await db.execute(select(AppUser).where(AppUser.google_sub == profile["sub"]))
    user = result.scalar_one_or_none()
    now = _now_utc()
    if user is None:
        user = AppUser(
            google_sub=profile["sub"],
            email=profile.get("email"),
            email_verified=bool(profile.get("email_verified")),
            full_name=profile.get("name"),
            given_name=profile.get("given_name"),
            family_name=profile.get("family_name"),
            picture_url=profile.get("picture"),
            locale=profile.get("locale"),
            is_active=True,
            last_login_at=now,
        )
        db.add(user)
    else:
        user.email = profile.get("email")
        user.email_verified = bool(profile.get("email_verified"))
        user.full_name = profile.get("name")
        user.given_name = profile.get("given_name")
        user.family_name = profile.get("family_name")
        user.picture_url = profile.get("picture")
        user.locale = profile.get("locale")
        user.is_active = True
        user.last_login_at = now

    await db.flush()
    return user


async def create_auth_session(db: AsyncSession, request: Request, user: AppUser) -> tuple[AuthSession, str]:
    raw_token = secrets.token_urlsafe(48)
    csrf_token = secrets.token_urlsafe(32)
    session = AuthSession(
        user_id=user.id,
        session_token_hash=_hash_value(raw_token),
        csrf_token=csrf_token,
        expires_at=_now_utc() + timedelta(hours=settings.auth_session_ttl_hours),
        last_seen_at=_now_utc(),
        ip_hash=_hash_value(request.client.host) if request.client and request.client.host else None,
        user_agent=request.headers.get("user-agent"),
    )
    db.add(session)
    await db.commit()
    await db.refresh(session)
    return session, raw_token


def attach_auth_cookie(response: JSONResponse | RedirectResponse, raw_token: str) -> None:
    max_age = int(settings.auth_session_ttl_hours * 3600)
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=raw_token,
        max_age=max_age,
        httponly=True,
        secure=_auth_cookie_secure(),
        samesite="lax",
        path="/",
    )


def clear_auth_cookie(response: JSONResponse | RedirectResponse) -> None:
    response.delete_cookie(
        key=settings.auth_cookie_name,
        path="/",
        secure=_auth_cookie_secure(),
        samesite="lax",
    )


async def resolve_google_callback(
    request: Request,
    db: AsyncSession,
    *,
    code: str | None,
    state: str | None,
    error: str | None = None,
) -> RedirectResponse:
    if error:
        return RedirectResponse(url=f"{settings.auth_login_success_redirect}?auth_error={error}", status_code=302)

    pending = request.session.pop("google_oauth", None)
    if not pending or not code or not state:
        return RedirectResponse(url=f"{settings.auth_login_success_redirect}?auth_error=missing_google_state", status_code=302)

    created_at = pending.get("created_at")
    try:
        created_at_dt = datetime.fromisoformat(created_at)
    except Exception:
        created_at_dt = _now_utc() - timedelta(minutes=settings.auth_state_ttl_minutes + 1)

    if pending.get("state") != state or (_now_utc() - created_at_dt) > timedelta(minutes=settings.auth_state_ttl_minutes):
        return RedirectResponse(url=f"{settings.auth_login_success_redirect}?auth_error=invalid_google_state", status_code=302)

    try:
        token_payload = await _exchange_google_code(request, code, pending["code_verifier"])
        profile = await _fetch_google_userinfo(token_payload["access_token"])
        user = await _upsert_google_user(db, profile)
        session, raw_token = await create_auth_session(db, request, user)
    except Exception:
        return RedirectResponse(url=f"{settings.auth_login_success_redirect}?auth_error=google_login_failed", status_code=302)

    response = RedirectResponse(url=pending.get("next") or settings.auth_login_success_redirect, status_code=302)
    attach_auth_cookie(response, raw_token)
    return response


async def get_auth_context(request: Request, db: AsyncSession) -> AuthContext:
    cached = getattr(request.state, "auth_context", None)
    if cached is not None:
        return cached

    if _is_testing_bypass_enabled():
        context = _testing_auth_context()
        request.state.auth_context = context
        return context

    raw_token = request.cookies.get(settings.auth_cookie_name)
    if not raw_token:
        raise HTTPException(status_code=401, detail="Autenticacion requerida")

    session_hash = _hash_value(raw_token)
    result = await db.execute(
        select(AuthSession, AppUser)
        .join(AppUser, AppUser.id == AuthSession.user_id)
        .where(AuthSession.session_token_hash == session_hash)
    )
    row = result.first()
    if row is None:
        raise HTTPException(status_code=401, detail="Sesion invalida")

    auth_session, user = row
    expires_at = _coerce_utc(auth_session.expires_at)
    last_seen_at = _coerce_utc(auth_session.last_seen_at) or _now_utc()
    if auth_session.revoked_at is not None or (expires_at is not None and expires_at <= _now_utc()):
        raise HTTPException(status_code=401, detail="Sesion expirada")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Usuario inactivo")

    if (_now_utc() - last_seen_at) > timedelta(minutes=10):
        auth_session.last_seen_at = _now_utc()
        await db.commit()

    context = AuthContext(user=user, session=auth_session, csrf_token=auth_session.csrf_token)
    request.state.auth_context = context
    return context


async def require_auth_context(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> AuthContext:
    return await get_auth_context(request, db)


async def require_authenticated_request(
    request: Request,
    auth: AuthContext = Depends(require_auth_context),
) -> AuthContext:
    if auth.synthetic:
        return auth
    if request.method.upper() not in SAFE_HTTP_METHODS:
        received = request.headers.get(settings.auth_csrf_header_name)
        if not received or received != auth.csrf_token:
            raise HTTPException(status_code=403, detail="CSRF token invalido")
    return auth


async def logout_auth_session(request: Request, db: AsyncSession) -> JSONResponse:
    if _is_testing_bypass_enabled():
        response = JSONResponse({"status": "ok", "logged_out": True})
        clear_auth_cookie(response)
        return response

    raw_token = request.cookies.get(settings.auth_cookie_name)
    if raw_token:
        session_hash = _hash_value(raw_token)
        result = await db.execute(select(AuthSession).where(AuthSession.session_token_hash == session_hash))
        auth_session = result.scalar_one_or_none()
        if auth_session and auth_session.revoked_at is None:
            auth_session.revoked_at = _now_utc()
            await db.commit()

    response = JSONResponse({"status": "ok", "logged_out": True})
    clear_auth_cookie(response)
    request.session.pop("google_oauth", None)
    return response


async def current_user_payload(request: Request, db: AsyncSession) -> dict[str, Any]:
    auth = await get_auth_context(request, db)
    expires_at = _coerce_utc(auth.session.expires_at).isoformat() if auth.session and _coerce_utc(auth.session.expires_at) else None
    profile_status = await get_profile_status(db, auth.user.id)
    return {
        "authenticated": True,
        "user": _serialize_user(auth.user),
        "csrf_token": auth.csrf_token,
        "expires_at": expires_at,
        "session_cookie_name": settings.auth_cookie_name,
        "profile_status": profile_status,
    }
