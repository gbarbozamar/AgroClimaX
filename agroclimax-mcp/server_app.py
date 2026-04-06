from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import settings
from mcp_server import mcp


def _extract_bearer_token(request: Request) -> str | None:
    raw_header = (request.headers.get("authorization") or "").strip()
    if not raw_header:
        return None
    scheme, _, token = raw_header.partition(" ")
    if scheme.lower() != "bearer":
        return None
    normalized = token.strip()
    return normalized or None


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(title="AgroClimaX MCP", version="0.1.0", lifespan=lifespan)
_mcp_http_app = mcp.streamable_http_app()


@app.middleware("http")
async def require_client_bearer(request: Request, call_next):
    if request.scope.get("path") == "/mcp":
        request.scope["path"] = "/mcp/"
        request.scope["raw_path"] = b"/mcp/"
    if request.url.path == "/healthz":
        return await call_next(request)
    if not settings.mcp_client_bearer_tokens:
        return JSONResponse({"detail": "MCP client bearer tokens no configurados"}, status_code=503)
    token = _extract_bearer_token(request)
    if token is None or token not in settings.mcp_client_bearer_tokens:
        return JSONResponse({"detail": "Bearer token invalido para MCP"}, status_code=401)
    return await call_next(request)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "agroclimax-mcp"}


app.mount("/mcp", _mcp_http_app)
app.mount("/", _mcp_http_app)
