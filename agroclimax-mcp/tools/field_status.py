from __future__ import annotations

import asyncio

from mcp.server.fastmcp import FastMCP

from agroclimax_client import AgroClimaXClient
from tools.common import ToolPayloadResult, tool_result


def _driver_preview(status_payload: dict) -> str:
    drivers = status_payload.get("status", {}).get("drivers") or []
    if not drivers:
        return "sin drivers priorizados"
    top = drivers[:3]
    return ", ".join(str(item.get("name") or item.get("key") or "driver") for item in top)


def register_field_status_tools(mcp: FastMCP, client: AgroClimaXClient) -> None:
    @mcp.tool(name="get_field_current_status", description="Devuelve el estado actual de un campo: riesgo, drivers y metricas principales.", structured_output=True)
    async def get_field_current_status(field_id: str) -> ToolPayloadResult:
        payload = await client.get_field_current_status(field_id)
        status = payload.get("status") or {}
        field = payload.get("field") or {}
        text = (
            f"Campo {field.get('name') or field_id}: estado {status.get('state')}, "
            f"riesgo {status.get('risk_score')} /100, confianza {status.get('confidence_score')} /100, "
            f"drivers principales: {_driver_preview(payload)}."
        )
        return tool_result(text=text, payload=payload)

    @mcp.tool(name="get_latest_satellite_coverage", description="Devuelve la ultima fecha util por capa satelital y metadatos de nubosidad/cobertura.", structured_output=True)
    async def get_latest_satellite_coverage(field_id: str) -> ToolPayloadResult:
        payload = await client.get_latest_satellite_coverage(field_id)
        layers = payload.get("layers") or []
        if not layers:
            text = f"No hay snapshots satelitales materializados para el campo {field_id}."
        else:
            latest = layers[0]
            text = (
                f"Ultima cobertura satelital para {payload.get('selection_label')}: "
                f"{payload.get('latest_observed_date')}. Primera capa reportada: {latest.get('layer_id')} "
                f"con estado visual {latest.get('visual_state')} y nubes {latest.get('cloud_pixel_pct')}%."
            )
        return tool_result(text=text, payload=payload)

    @mcp.tool(name="summarize_field_risk", description="Compone un resumen ejecutivo deterministico del riesgo actual del campo.", structured_output=True)
    async def summarize_field_risk(field_id: str) -> ToolPayloadResult:
        current = await asyncio.wait_for(client.get_field_current_status(field_id), timeout=15)
        coverage = await asyncio.wait_for(client.get_latest_satellite_coverage(field_id), timeout=15)

        status = current.get("status") or {}
        field = current.get("field") or {}
        payload_date = coverage.get("latest_observed_date")
        total_alerts = 1 if bool(status.get("actionable")) else 0

        summary_text = (
            f"Resumen ejecutivo para {field.get('name') or field_id}: estado {status.get('state')} "
            f"con riesgo {status.get('risk_score')} /100 y confianza {status.get('confidence_score')} /100. "
            f"Drivers dominantes: {_driver_preview(current)}. "
            f"Alertas activas: {total_alerts}. "
            f"Ultima cobertura satelital: {payload_date}."
        )
        top_drivers = [
            {
                "name": item.get("name") or item.get("key"),
                "score": item.get("score"),
                "detail": item.get("detail"),
            }
            for item in (status.get("drivers") or [])[:5]
        ]
        payload = {
            "field_id": field_id,
            "field_name": field.get("name"),
            "state": status.get("state"),
            "risk_score": status.get("risk_score"),
            "confidence_score": status.get("confidence_score"),
            "top_drivers": top_drivers,
            "total_active_weather_alerts": total_alerts,
            "latest_satellite_observed_at": payload_date,
            "summary_text": summary_text,
        }
        return tool_result(text=summary_text, payload=payload)
