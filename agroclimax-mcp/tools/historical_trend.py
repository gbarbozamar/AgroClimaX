from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agroclimax_client import AgroClimaXClient
from tools.common import ToolPayloadResult, tool_result


def _series_summary(payload: dict, preferred_key: str) -> str:
    series = (payload.get("series") or {}).get(preferred_key) or {}
    if not series.get("available"):
        return f"{preferred_key} no materializado"
    points = series.get("points") or []
    if not points:
        return f"{preferred_key} sin puntos"
    last = points[-1]
    return f"{preferred_key} ultimo valor {last.get('value')} en {last.get('date')}"


def register_historical_trend_tools(mcp: FastMCP, client: AgroClimaXClient) -> None:
    @mcp.tool(name="get_field_historical_trend", description="Devuelve la serie historica consolidada de indices para un campo.", structured_output=True)
    async def get_field_historical_trend(field_id: str, days: int = 30) -> ToolPayloadResult:
        payload = await client.get_field_historical_trend(field_id, days=days)
        text = (
            f"Tendencia historica de {payload.get('selection_label')}: "
            f"{_series_summary(payload, 'ndmi')}; {_series_summary(payload, 'sar_vv_db')}. "
            f"Series faltantes: {', '.join(payload.get('missing_series') or []) or 'ninguna'}."
        )
        return tool_result(text=text, payload=payload)

    @mcp.tool(name="get_paddock_historical_trend", description="Devuelve la serie historica consolidada de indices para un potrero.", structured_output=True)
    async def get_paddock_historical_trend(paddock_id: str, days: int = 30) -> ToolPayloadResult:
        payload = await client.get_paddock_historical_trend(paddock_id, days=days)
        text = (
            f"Tendencia historica de {payload.get('selection_label')}: "
            f"{_series_summary(payload, 'ndmi')}; {_series_summary(payload, 'sar_vv_db')}. "
            f"Series faltantes: {', '.join(payload.get('missing_series') or []) or 'ninguna'}."
        )
        return tool_result(text=text, payload=payload)

    @mcp.tool(name="get_paddock_alert_history", description="Devuelve el historial de alertas de un potrero para responder preguntas de ocurrencia reciente.", structured_output=True)
    async def get_paddock_alert_history(paddock_id: str, days: int = 30) -> ToolPayloadResult:
        payload = await client.get_paddock_alert_history(paddock_id, days=days)
        history = (payload.get("alert_history") or {}).get("datos") or []
        if not history:
            text = f"No hay alertas historicas en los ultimos {days} dias para el potrero {payload.get('selection_label') or paddock_id}."
        else:
            latest = history[0]
            text = (
                f"Historial de alertas de {payload.get('selection_label')}: {len(history)} eventos en {days} dias. "
                f"Ultimo estado {latest.get('state')} el {latest.get('fecha')} con riesgo {latest.get('risk_score')}."
            )
        return tool_result(text=text, payload=payload)
