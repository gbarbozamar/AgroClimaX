"""
Endpoints de autenticación con Google OAuth2.
"""
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from jose import jwt, JWTError
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from app.core.config import settings
from app.core.security import create_access_token
from app.db.session import AsyncSessionLocal
from app.models.user import User

router = APIRouter(prefix="/auth", tags=["auth"])

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


def _build_redirect_uri(request: Request) -> str:
    """Build the OAuth callback URI from the incoming request."""
    base = str(request.base_url).rstrip("/")
    return f"{base}{settings.api_prefix}/auth/google/callback"


def _encode_state(redirect_uri: str) -> str:
    """Encode redirect_uri and a nonce into a short-lived JWT (state param)."""
    payload = {
        "nonce": secrets.token_hex(16),
        "redirect_uri": redirect_uri,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=10),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def _decode_state(state: str) -> dict:
    """Decode and verify the state JWT. Raises HTTPException on failure."""
    try:
        return jwt.decode(
            state, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm]
        )
    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid or expired state parameter")


@router.get("/google/login")
async def google_login(request: Request):
    """Redirect the user to Google's OAuth consent screen."""
    redirect_uri = _build_redirect_uri(request)
    state = _encode_state(redirect_uri)

    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "consent",
    }
    return RedirectResponse(url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}", status_code=307)


@router.get("/google/callback")
async def google_callback(request: Request, code: str, state: str):
    """Handle Google's OAuth callback, upsert user, and issue JWT."""
    state_data = _decode_state(state)
    redirect_uri = state_data["redirect_uri"]

    # Exchange authorization code for tokens
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to exchange authorization code")
        tokens = token_resp.json()

        # Fetch user info
        userinfo_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        if userinfo_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to fetch user info from Google")
        userinfo = userinfo_resp.json()

    # Upsert user in database
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(User).where(User.google_id == userinfo["id"])
        )
        user = result.scalar_one_or_none()

        now = datetime.now(timezone.utc)
        if user:
            user.name = userinfo.get("name", user.name)
            user.picture_url = userinfo.get("picture")
            user.email = userinfo.get("email", user.email)
            user.last_login = now
        else:
            user = User(
                google_id=userinfo["id"],
                email=userinfo["email"],
                name=userinfo.get("name", ""),
                picture_url=userinfo.get("picture"),
                created_at=now,
                last_login=now,
            )
            db.add(user)

        await db.commit()
        await db.refresh(user)

    # Create application JWT
    app_token = create_access_token(
        data={
            "sub": user.id,
            "email": user.email,
            "name": user.name,
            "picture": user.picture_url or "",
        }
    )

    # Redirect to frontend with token in URL fragment
    frontend = settings.frontend_url.rstrip("/") if settings.frontend_url else str(request.base_url).rstrip("/")
    return RedirectResponse(url=f"{frontend}/#token={app_token}", status_code=302)
