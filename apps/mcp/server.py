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
async def paddock_metrics(paddock_id: str, date_range_days: int = 30, user_id: str | None = None) -> dict:
    """Métricas agregadas de un potrero (risk current/mean/max 30d, NDMI trend, días en alerta)."""
    return await _get_or_placeholder(
        f"/api/v1/mcp/paddocks/{paddock_id}/metrics",
        {"date_range_days": date_range_days},
        user_id,
    )


@mcp.tool
async def establishment_summary(establishment_id: str, user_id: str | None = None) -> dict:
    """Resumen de establecimiento (fields totales, área, highest_risk_field, fields_in_alert)."""
    return await _get_or_placeholder(
        f"/api/v1/mcp/establishments/{establishment_id}/summary", {}, user_id
    )


@mcp.tool
async def crop_prediction(
    field_id: str, horizon_days: int = 30, user_id: str | None = None
) -> dict:
    """Predicción heurística del outlook del campo (NDMI trend + risk tier + yield estimate). Modelo: heuristic-v0.1."""
    return await _get_or_placeholder(
        f"/api/v1/mcp/fields/{field_id}/crop-prediction",
        {"horizon_days": horizon_days},
        user_id,
    )


@mcp.tool
async def get_video_status(field_id: str, job_id: str, user_id: str | None = None) -> dict:
    """Estado de un video job: queued/rendering/ready/failed + progress + video_url."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/video/{job_id}", headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def list_video_jobs(field_id: str, limit: int = 20, user_id: str | None = None) -> dict:
    """Lista los N video jobs más recientes de un campo."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/videos", params={"limit": limit}, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def list_user_fields(user_id: str) -> dict:
    """Lista todos los campos de un usuario con su metadata básica."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/users/{user_id}/fields", headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def get_field_details(field_id: str, user_id: str | None = None) -> dict:
    """Detalles completos de un campo incluyendo paddocks."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/details", headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def list_paddocks(field_id: str, user_id: str | None = None) -> dict:
    """Lista los paddocks de un campo."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/paddocks", headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def list_establishments(user_id: str) -> dict:
    """Lista los establecimientos de un usuario."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/users/{user_id}/establishments", headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def get_alert_current(scope: str = "nacional", ref: str | None = None, user_id: str | None = None) -> dict:
    """Estado actual de alertas. scope: nacional|departamento|unidad|field. ref: name/id según scope."""
    async with httpx.AsyncClient() as cli:
        params: dict = {"scope": scope}
        if ref: params["ref"] = ref
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/alerts/current", params=params, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def get_alert_history(scope: str = "nacional", ref: str | None = None, limit: int = 30, user_id: str | None = None) -> dict:
    """Histórico de alertas. Últimos N estados del scope."""
    async with httpx.AsyncClient() as cli:
        params: dict = {"scope": scope, "limit": limit}
        if ref: params["ref"] = ref
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/alerts/history", params=params, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def get_alert_forecast(scope: str = "nacional", ref: str | None = None, user_id: str | None = None) -> dict:
    """Forecast agroclimático (proyección de alertas a N días) si hay ForecastSignal disponible."""
    async with httpx.AsyncClient() as cli:
        params: dict = {"scope": scope}
        if ref: params["ref"] = ref
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/alerts/forecast", params=params, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def layers_available(field_id: str, user_id: str | None = None) -> dict:
    """Capas con snapshots rendereados para un campo."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/layers-available", headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def trigger_backfill(field_id: str, days: int = 30, layers: list[str] | None = None, user_id: str | None = None) -> dict:
    """Dispara backfill async de snapshots para N días y M capas."""
    body = {"days": days, "layers": layers or ["ndvi", "ndmi", "alerta_fusion"]}
    async with httpx.AsyncClient() as cli:
        r = await cli.post(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/backfill", json=body, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()


@mcp.tool
async def download_snapshot_url(field_id: str, storage_key: str, user_id: str | None = None) -> dict:
    """Retorna la URL directa del PNG de un snapshot (Claude puede luego descargarla)."""
    return {
        "url": f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/snapshots/{storage_key}",
        "requires_header": {"X-Service-Token": "..."},
        "note": "Fetch con httpx.get pasando el X-Service-Token header para obtener los bytes del PNG",
    }


if __name__ == "__main__":
    transport = os.environ.get("AGROCLIMAX_MCP_TRANSPORT", "stdio").lower()
    if transport == "http":
        # Port priority: Railway PORT > MCP_SERVER_PORT > AGROCLIMAX_MCP_PORT > 8088.
        # Railway injects PORT automatically on every deploy; respetarlo es
        # condición necesaria para que el gateway público llegue al container.
        port = int(
            os.environ.get("PORT")
            or os.environ.get("MCP_SERVER_PORT")
            or os.environ.get("AGROCLIMAX_MCP_PORT")
            or "8088"
        )
        # Bind 0.0.0.0 (no 127.0.0.1) para ser alcanzable desde fuera del
        # container. FastMCP default es 127.0.0.1 que solo funciona local.
        host = os.environ.get("AGROCLIMAX_MCP_HOST", "0.0.0.0")
        mcp.run(transport="http", host=host, port=port)
    else:
        mcp.run(transport="stdio")
