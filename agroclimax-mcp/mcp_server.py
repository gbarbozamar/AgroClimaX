from __future__ import annotations

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

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


def _build_allowed_hosts() -> list[str]:
    allowed_hosts = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
    for host in settings.mcp_allowed_hosts:
        normalized = host.strip()
        if not normalized:
            continue
        allowed_hosts.append(normalized)
        if ":" not in normalized and "*" not in normalized:
            allowed_hosts.append(f"{normalized}:*")
    deduped: list[str] = []
    seen: set[str] = set()
    for host in allowed_hosts:
        if host in seen:
            continue
        seen.add(host)
        deduped.append(host)
    return deduped


transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=_build_allowed_hosts(),
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
    transport_security=transport_security,
)

register_search_tools(mcp, client)
register_field_status_tools(mcp, client)
register_historical_trend_tools(mcp, client)
register_weather_alert_tools(mcp, client)
