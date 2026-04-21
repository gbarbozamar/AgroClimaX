"""AgroClimaX MCP server.

Exposes the AgroClimaX backend (FastAPI) as MCP tools so Claude Desktop / other
MCP clients can introspect fields, timelines, video jobs, and alerting state.

Transport:
  - `stdio` (default) — what Claude Desktop expects when spawning the server.
  - `http`            — set AGROCLIMAX_MCP_TRANSPORT=http to expose HTTP on
                        AGROCLIMAX_MCP_PORT (default 8088) for remote clients.

Env:
  AGROCLIMAX_BACKEND_URL   backend base URL (default http://127.0.0.1:8001)
  MCP_SERVICE_TOKEN        service token sent as X-Service-Token
  AGROCLIMAX_MCP_TRANSPORT  "stdio" (default) or "http"
  AGROCLIMAX_MCP_PORT      port for http transport (default 8088)
"""
from __future__ import annotations

import os

import httpx
from fastmcp import FastMCP

BACKEND_URL = os.environ.get("AGROCLIMAX_BACKEND_URL", "http://127.0.0.1:8001")
SERVICE_TOKEN = os.environ.get("MCP_SERVICE_TOKEN", "")

mcp = FastMCP("agroclimax")


def _headers(user_id: str | None = None) -> dict:
    h = {"X-Service-Token": SERVICE_TOKEN}
    if user_id:
        h["X-User-Id"] = user_id
    return h


@mcp.tool
async def get_field_snapshot(
    field_id: str,
    layer: str = "ndvi",
    date: str | None = None,
    user_id: str | None = None,
) -> dict:
    """Retorna snapshot PNG + metadata de un campo en una fecha."""
    async with httpx.AsyncClient() as cli:
        params: dict[str, str] = {"layer": layer}
        if date:
            params["date"] = date
        r = await cli.get(
            f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/snapshot",
            params=params,
            headers=_headers(user_id),
        )
        r.raise_for_status()
        return r.json()


@mcp.tool
async def get_field_timeline(
    field_id: str,
    layer: str = "ndvi",
    days: int = 30,
    user_id: str | None = None,
) -> dict:
    """Retorna últimos N snapshots del campo."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(
            f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/timeline",
            params={"layer": layer, "days": days},
            headers=_headers(user_id),
        )
        r.raise_for_status()
        return r.json()


@mcp.tool
async def request_field_video(
    field_id: str,
    layer: str = "ndvi",
    duration_days: int = 30,
    user_id: str | None = None,
) -> dict:
    """Solicita video timelapse. Retorna job_id + status."""
    async with httpx.AsyncClient() as cli:
        r = await cli.post(
            f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/video",
            json={"layer_key": layer, "duration_days": duration_days},
            headers=_headers(user_id),
        )
        r.raise_for_status()
        return r.json()


@mcp.tool
async def list_fields_by_alert(min_level: int = 2, user_id: str | None = None) -> list:
    """Lista campos con alerta >= nivel."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(
            f"{BACKEND_URL}/api/v1/mcp/fields/by-alert",
            params={"min_level": min_level},
            headers=_headers(user_id),
        )
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# Placeholder tools for Fase 5 paddock intelligence services.
#
# Backend services exist under apps/backend/app/services/ (paddock_metrics,
# establishment_summary, crop_prediction) but their MCP-facing endpoints are
# still being added by other agents working in parallel. These placeholders
# let Claude Desktop discover the tools today; they gracefully degrade to a
# "not_implemented" payload if the backend route is not wired yet.
# ---------------------------------------------------------------------------


async def _get_or_placeholder(path: str, params: dict, user_id: str | None) -> dict:
    async with httpx.AsyncClient() as cli:
        try:
            r = await cli.get(f"{BACKEND_URL}{path}", params=params, headers=_headers(user_id))
        except httpx.HTTPError as exc:
            return {"status": "not_implemented", "reason": f"backend unreachable: {exc}"}
        if r.status_code == 404:
            return {"status": "not_implemented", "reason": "endpoint not wired yet"}
        r.raise_for_status()
        return r.json()


@mcp.tool
async def paddock_metrics(field_id: str, user_id: str | None = None) -> dict:
    """Métricas agronómicas por potrero (NDVI/NDMI stats, área, heterogeneidad)."""
    return await _get_or_placeholder(
        f"/api/v1/mcp/fields/{field_id}/paddock-metrics", {}, user_id
    )


@mcp.tool
async def establishment_summary(field_id: str, user_id: str | None = None) -> dict:
    """Resumen de establecimiento (emergencia, cobertura, stand count estimado)."""
    return await _get_or_placeholder(
        f"/api/v1/mcp/fields/{field_id}/establishment-summary", {}, user_id
    )


@mcp.tool
async def crop_prediction(
    field_id: str, horizon_days: int = 30, user_id: str | None = None
) -> dict:
    """Predicción de rendimiento / estrés hídrico a N días."""
    return await _get_or_placeholder(
        f"/api/v1/mcp/fields/{field_id}/crop-prediction",
        {"horizon_days": horizon_days},
        user_id,
    )


if __name__ == "__main__":
    transport = os.environ.get("AGROCLIMAX_MCP_TRANSPORT", "stdio").lower()
    if transport == "http":
        port = int(os.environ.get("AGROCLIMAX_MCP_PORT", "8088"))
        mcp.run(transport="http", port=port)
    else:
        mcp.run(transport="stdio")
