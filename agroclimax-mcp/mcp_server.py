from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agroclimax_client import AgroClimaXClient
from config import settings
from tools.field_status import register_field_status_tools
from tools.historical_trend import register_historical_trend_tools
from tools.search import register_search_tools
from tools.weather_alerts import register_weather_alert_tools


client = AgroClimaXClient(
    base_url=settings.agroclimax_api_url,
    api_key=settings.agroclimax_api_key,
)

mcp = FastMCP(
    name="AgroClimaX MCP",
    instructions=(
        "Servidor MCP de solo lectura para consultar estado hidrico, tendencias historicas, "
        "cobertura satelital y alertas climaticas de campos y potreros en AgroClimaX."
    ),
    json_response=True,
    stateless_http=True,
    streamable_http_path="/",
    mount_path="/",
)

register_search_tools(mcp, client)
register_field_status_tools(mcp, client)
register_historical_trend_tools(mcp, client)
register_weather_alert_tools(mcp, client)
