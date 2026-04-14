from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agroclimax_client import AgroClimaXClient
from tools.common import ToolPayloadResult, tool_result


def register_weather_alert_tools(mcp: FastMCP, client: AgroClimaXClient) -> None:
    @mcp.tool(name="get_active_weather_alerts", description="Devuelve alertas climaticas activas o proyectadas para un campo.", structured_output=True)
    async def get_active_weather_alerts(field_id: str) -> ToolPayloadResult:
        payload = await client.get_active_weather_alerts(field_id)
        alerts = payload.get("alerts") or []
        if not alerts:
            text = f"No hay alertas climaticas activas para {payload.get('selection_label') or field_id}."
        else:
            preview = ", ".join(
                f"{item.get('type')} ({item.get('severity')})"
                for item in alerts[:3]
            )
            text = f"Hay {len(alerts)} alertas activas para {payload.get('selection_label')}: {preview}."
        return tool_result(text=text, payload=payload)
