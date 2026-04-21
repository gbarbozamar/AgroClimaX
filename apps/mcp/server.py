from fastmcp import FastMCP
import os
import httpx

BACKEND_URL = os.environ.get("AGROCLIMAX_BACKEND_URL", "http://127.0.0.1:8001")
SERVICE_TOKEN = os.environ.get("MCP_SERVICE_TOKEN", "")

mcp = FastMCP("agroclimax")

def _headers(user_id: str | None = None) -> dict:
    h = {"X-Service-Token": SERVICE_TOKEN}
    if user_id:
        h["X-User-Id"] = user_id
    return h

@mcp.tool
async def get_field_snapshot(field_id: str, layer: str = "ndvi", date: str | None = None, user_id: str | None = None) -> dict:
    """Retorna snapshot PNG + metadata de un campo en una fecha."""
    async with httpx.AsyncClient() as cli:
        params = {"layer": layer}
        if date: params["date"] = date
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/snapshot", params=params, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()

@mcp.tool
async def get_field_timeline(field_id: str, layer: str = "ndvi", days: int = 30, user_id: str | None = None) -> dict:
    """Retorna ultimos N snapshots del campo."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/timeline", params={"layer": layer, "days": days}, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()

@mcp.tool
async def request_field_video(field_id: str, layer: str = "ndvi", duration_days: int = 30, user_id: str | None = None) -> dict:
    """Solicita video timelapse. Retorna job_id + status."""
    async with httpx.AsyncClient() as cli:
        r = await cli.post(f"{BACKEND_URL}/api/v1/mcp/fields/{field_id}/video", json={"layer_key": layer, "duration_days": duration_days}, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()

@mcp.tool
async def list_fields_by_alert(min_level: int = 2, user_id: str | None = None) -> list:
    """Lista campos con alerta >= nivel."""
    async with httpx.AsyncClient() as cli:
        r = await cli.get(f"{BACKEND_URL}/api/v1/mcp/fields/by-alert", params={"min_level": min_level}, headers=_headers(user_id))
        r.raise_for_status()
        return r.json()

if __name__ == "__main__":
    mcp.run(transport="http", port=8088)
