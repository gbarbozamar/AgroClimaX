from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agroclimax_client import AgroClimaXClient
from tools.common import ToolPayloadResult, tool_result


def register_search_tools(mcp: FastMCP, client: AgroClimaXClient) -> None:
    @mcp.tool(name="find_field", description="Busca campos por nombre y devuelve coincidencias con metadata operativa.", structured_output=True)
    async def find_field(name: str, limit: int = 5) -> ToolPayloadResult:
        payload = await client.find_field(name, limit=limit)
        items = payload.get("items") or []
        if not items:
            text = f"No se encontraron campos para '{name}'."
        else:
            preview = ", ".join(f"{item.get('name')} ({item.get('id')})" for item in items[:3])
            text = f"Se encontraron {len(items)} campos. Principales coincidencias: {preview}."
        return tool_result(text=text, payload=payload)

    @mcp.tool(name="find_paddock", description="Busca potreros por nombre, con filtro opcional por campo.", structured_output=True)
    async def find_paddock(name: str, field_id: str | None = None, limit: int = 10) -> ToolPayloadResult:
        payload = await client.find_paddock(name, field_id=field_id, limit=limit)
        items = payload.get("items") or []
        if not items:
            text = f"No se encontraron potreros para '{name}'."
        else:
            preview = ", ".join(f"{item.get('name')} ({item.get('id')})" for item in items[:3])
            text = f"Se encontraron {len(items)} potreros. Principales coincidencias: {preview}."
        return tool_result(text=text, payload=payload)
