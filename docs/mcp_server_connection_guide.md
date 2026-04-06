# Guia de conexion al MCP Server de AgroClimaX

## 1. Objetivo

Este documento describe como conectar otra aplicacion, agente o LLM al MCP Server de AgroClimaX para consultar informacion operativa de campos y potreros sin scrappear el frontend web.

El servidor MCP expone herramientas de solo lectura sobre:

- estado actual del riesgo de un campo
- tendencias historicas de indices
- cobertura satelital mas reciente
- alertas climaticas activas
- historial reciente de alertas por potrero
- busqueda de campos y potreros por nombre

## 2. Estado actual de despliegue

### Endpoint productivo activo

- MCP health: `https://agroclimax-mcp-production.up.railway.app/healthz`
- MCP endpoint: `https://agroclimax-mcp-production.up.railway.app/mcp`

### Dominio custom

El servicio fue preparado para usar:

- `https://mcp.agroclimax.com/mcp`

Estado actual:

- el dominio custom fue creado en Railway
- todavia depende de completar DNS externo
- hasta que eso ocurra, el endpoint operativo es el dominio Railway

## 3. Modelo de autenticacion

### 3.1 Cliente externo -> MCP Server

El MCP Server exige:

- `Authorization: Bearer <MCP_CLIENT_TOKEN>`

Ese token lo valida el propio servicio MCP y **no** debe exponerse en clientes publicos o frontend browser.

### 3.2 MCP Server -> Backend AgroClimaX

El MCP Server consulta la API interna de integracion de AgroClimaX usando:

- `Authorization: Bearer <AGROCLIMAX_API_KEY>`

Ese bearer es un **service token interno** y tampoco debe compartirse fuera del backend/MCP.

### 3.3 Seguridad operativa

- no incluir tokens reales en codigo fuente
- inyectar los tokens por variables de entorno
- rotar ambos tokens si se comparten con terceros o aparecen en logs
- no usar los endpoints `/api/v1/integrations/mcp/*` directamente desde clientes no confiables

## 4. Transporte MCP

El servidor usa **Model Context Protocol** sobre **Streamable HTTP**.

Ruta de transporte:

- `POST /mcp`

Healthcheck:

- `GET /healthz`

### Headers recomendados

```http
Authorization: Bearer <MCP_CLIENT_TOKEN>
Accept: application/json, text/event-stream
Content-Type: application/json
```

## 5. Flujo recomendado de uso

La secuencia mas robusta para un agente es:

1. buscar el campo por nombre con `find_field`
2. tomar el `field_id` devuelto
3. consultar el estado actual con `get_field_current_status`
4. consultar alertas con `get_active_weather_alerts`
5. consultar tendencias con `get_field_historical_trend`
6. opcionalmente, consultar cobertura satelital con `get_latest_satellite_coverage`

Para potreros:

1. buscar el campo con `find_field`
2. buscar el potrero con `find_paddock(field_id=...)`
3. consultar `get_paddock_historical_trend`
4. consultar `get_paddock_alert_history`

## 6. Catalogo de herramientas expuestas

## `find_field`

Busca campos por nombre y devuelve coincidencias operativas.

### Parametros

```json
{
  "name": "El Trebol",
  "limit": 5
}
```

### Structured content esperado

```json
{
  "query": "El Trebol",
  "total": 1,
  "items": [
    {
      "id": "farm-field-123",
      "name": "El Trebol",
      "department": "Rivera",
      "aoi_unit_id": "productive-unit-abc",
      "area_ha": 124.5,
      "match_score": 1.0
    }
  ]
}
```

## `find_paddock`

Busca potreros por nombre, con filtro opcional por `field_id`.

### Parametros

```json
{
  "name": "Potrero 5",
  "field_id": "farm-field-123",
  "limit": 10
}
```

### Structured content esperado

```json
{
  "query": "Potrero 5",
  "field_id": "farm-field-123",
  "total": 1,
  "items": [
    {
      "id": "paddock-xyz",
      "name": "Potrero 5",
      "field_id": "farm-field-123",
      "field_name": "El Trebol",
      "aoi_unit_id": "productive-unit-potrero",
      "area_ha": 18.2,
      "match_score": 1.0
    }
  ]
}
```

## `get_field_current_status`

Devuelve el estado actual analitico de un campo.

### Parametros

```json
{
  "field_id": "farm-field-123"
}
```

### Structured content esperado

```json
{
  "scope_type": "field",
  "scope_id": "farm-field-123",
  "aoi_unit_id": "productive-unit-abc",
  "selection_label": "El Trebol",
  "field": {
    "id": "farm-field-123",
    "name": "El Trebol",
    "department": "Rivera",
    "aoi_unit_id": "productive-unit-abc"
  },
  "status": {
    "state": "Vigilancia",
    "risk_score": 54.1,
    "confidence_score": 69.8,
    "drivers": [
      {
        "name": "spi_30d",
        "score": 71.0
      }
    ]
  }
}
```

## `get_field_historical_trend`

Serie historica consolidada de indices para un campo.

### Parametros

```json
{
  "field_id": "farm-field-123",
  "days": 30
}
```

### Campos principales del resultado

```json
{
  "scope_type": "field",
  "scope_id": "farm-field-123",
  "days": 30,
  "selection_label": "El Trebol",
  "latest_observed_date": "2026-04-05",
  "series": {
    "ndmi": {
      "available": true,
      "points": [
        {"date": "2026-04-01", "value": 0.12}
      ]
    },
    "sar_vv_db": {
      "available": true,
      "points": [
        {"date": "2026-04-01", "value": -14.8}
      ]
    }
  },
  "missing_series": ["ndvi"]
}
```

## `get_paddock_historical_trend`

Igual que la anterior, pero resolviendo un potrero.

### Parametros

```json
{
  "paddock_id": "paddock-xyz",
  "days": 30
}
```

## `get_latest_satellite_coverage`

Devuelve la ultima cobertura satelital util por capa para un campo.

### Parametros

```json
{
  "field_id": "farm-field-123"
}
```

### Structured content esperado

```json
{
  "field": {
    "id": "farm-field-123",
    "name": "El Trebol"
  },
  "aoi_unit_id": "productive-unit-abc",
  "selection_label": "El Trebol",
  "latest_observed_date": "2026-04-05",
  "layers": [
    {
      "layer_id": "ndmi",
      "primary_source_date": "2026-04-05",
      "cloud_pixel_pct": 12.4,
      "renderable_pixel_pct": 88.1,
      "visual_state": "ready"
    }
  ]
}
```

## `get_active_weather_alerts`

Devuelve alertas climaticas activas o proyectadas para un campo.

### Parametros

```json
{
  "field_id": "farm-field-123"
}
```

### Structured content esperado

```json
{
  "field": {
    "id": "farm-field-123",
    "name": "El Trebol"
  },
  "selection_label": "El Trebol",
  "alerts": [
    {
      "type": "sequia",
      "severity": "high",
      "title": "Deficit hidrico proyectado",
      "window_start": "2026-04-06",
      "window_end": "2026-04-10"
    }
  ]
}
```

## `get_paddock_alert_history`

Devuelve historial de alertas de un potrero.

### Parametros

```json
{
  "paddock_id": "paddock-xyz",
  "days": 30
}
```

### Structured content esperado

```json
{
  "scope_type": "paddock",
  "scope_id": "paddock-xyz",
  "selection_label": "Potrero 5",
  "days": 30,
  "alert_history": {
    "datos": [
      {
        "fecha": "2026-04-02",
        "state": "Vigilancia",
        "risk_score": 25
      }
    ]
  }
}
```

## `summarize_field_risk`

Resumen ejecutivo deterministico del riesgo actual del campo.

### Parametros

```json
{
  "field_id": "farm-field-123"
}
```

### Structured content esperado

```json
{
  "field_id": "farm-field-123",
  "field_name": "El Trebol",
  "state": "Vigilancia",
  "risk_score": 54.1,
  "confidence_score": 69.8,
  "top_drivers": [
    {
      "name": "spi_30d",
      "score": 71.0,
      "detail": null
    }
  ],
  "total_active_weather_alerts": 1,
  "latest_satellite_observed_at": "2026-04-05",
  "summary_text": "Resumen ejecutivo para El Trebol: estado Vigilancia con riesgo 54.1 /100 ..."
}
```

## 7. Mapeo interno tool -> endpoint backend

El MCP Server no inventa datos. Cada tool consulta endpoints internos del backend de AgroClimaX.

| Tool | Backend endpoint |
| --- | --- |
| `find_field` | `GET /api/v1/integrations/mcp/fields/search?q=...&limit=...` |
| `find_paddock` | `GET /api/v1/integrations/mcp/paddocks/search?q=...&field_id=...&limit=...` |
| `get_field_current_status` | `GET /api/v1/integrations/mcp/fields/{field_id}/current-status` |
| `get_field_historical_trend` | `GET /api/v1/integrations/mcp/fields/{field_id}/historical-trend?days=...` |
| `get_paddock_historical_trend` | `GET /api/v1/integrations/mcp/paddocks/{paddock_id}/historical-trend?days=...` |
| `get_latest_satellite_coverage` | `GET /api/v1/integrations/mcp/fields/{field_id}/latest-satellite-coverage` |
| `get_active_weather_alerts` | `GET /api/v1/integrations/mcp/fields/{field_id}/active-weather-alerts` |
| `get_paddock_alert_history` | `GET /api/v1/integrations/mcp/paddocks/{paddock_id}/alert-history?days=...` |
| `summarize_field_risk` | compone `get_field_current_status` + `get_latest_satellite_coverage` |

## 8. Ejemplo con cliente Python oficial

```python
import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

MCP_URL = "https://agroclimax-mcp-production.up.railway.app/mcp"
MCP_TOKEN = "reemplazar_por_token_real"

async def main():
    headers = {"Authorization": f"Bearer {MCP_TOKEN}"}
    async with streamablehttp_client(MCP_URL, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            matches = await session.call_tool("find_field", {"name": "El Trebol", "limit": 3})
            items = matches.structuredContent.get("items", [])
            if not items:
                print("Campo no encontrado")
                return

            field_id = items[0]["id"]

            status = await session.call_tool("get_field_current_status", {"field_id": field_id})
            trend = await session.call_tool("get_field_historical_trend", {"field_id": field_id, "days": 30})
            alerts = await session.call_tool("get_active_weather_alerts", {"field_id": field_id})

            print(status.structuredContent)
            print(trend.structuredContent)
            print(alerts.structuredContent)

asyncio.run(main())
```

## 9. Ejemplo de handshake HTTP crudo

Esto sirve para validar que el transporte MCP esta respondiendo.

```bash
curl -X POST "https://agroclimax-mcp-production.up.railway.app/mcp" ^
  -H "Authorization: Bearer <MCP_CLIENT_TOKEN>" ^
  -H "Content-Type: application/json" ^
  -H "Accept: application/json, text/event-stream" ^
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"smoke\",\"version\":\"1.0\"}}}"
```

Respuesta esperada:

- `HTTP 200`
- payload JSON-RPC con `protocolVersion`, `capabilities` y `serverInfo`

## 10. Variables de entorno del servicio MCP

Variables esperadas por el servicio:

- `AGROCLIMAX_API_URL`
- `AGROCLIMAX_API_KEY`
- `MCP_CLIENT_BEARER_TOKENS`
- `MCP_ALLOWED_HOSTS`
- `PORT` o `MCP_SERVER_PORT`

Ejemplo:

```env
AGROCLIMAX_API_URL=https://agroclimax-production-a43f.up.railway.app
AGROCLIMAX_API_KEY=service_token_interno
MCP_CLIENT_BEARER_TOKENS=token_cliente_1,token_cliente_2
MCP_ALLOWED_HOSTS=agroclimax-mcp-production.up.railway.app,mcp.agroclimax.com
MCP_SERVER_PORT=8090
```

## 11. Limitaciones actuales

- el servidor MCP esta operativo y validado publicamente
- el dominio Railway funciona hoy; el custom domain depende de DNS
- la instancia productiva actual no tiene cargados los campos de ejemplo locales usados durante desarrollo
- por eso, consultas genericas como `find_field("Campo")` pueden devolver `0` resultados hasta que se carguen datos reales de campos/potreros en produccion
- `summarize_field_risk` es deterministico y **no** usa otro LLM; compone estado actual + cobertura satelital

## 12. Manejo de errores esperado

### `401 Unauthorized`

Motivos comunes:

- falta `Authorization: Bearer ...`
- token MCP cliente invalido

### `200` con `total = 0`

Motivo:

- el MCP funciona, pero no encontro entidades en la base productiva con ese nombre

### `404`

Motivos comunes:

- `field_id` o `paddock_id` inexistente

### `5xx`

Motivos comunes:

- backend AgroClimaX no disponible
- token interno del MCP server hacia AgroClimaX invalido
- timeout transitorio aguas abajo

## 13. Recomendaciones para otro agente o app

- no asumir que el nombre del campo es unico; siempre resolver con `find_field` antes de consultar
- persistir el `field_id` o `paddock_id` una vez resuelto
- usar `structuredContent` como fuente primaria
- usar `content` solo como resumen legible
- tratar `missing_series` como estado valido, no como error
- si se usa el dominio custom, verificar primero que DNS ya este activo; hasta entonces usar el dominio Railway

## 14. Smoke test minimo recomendado

1. `GET /healthz`
2. `initialize`
3. `list_tools`
4. `find_field("...")`
5. si hay resultados, `get_field_current_status(field_id=...)`
6. `get_latest_satellite_coverage(field_id=...)`
7. `summarize_field_risk(field_id=...)`

## 15. Archivo fuente relacionado

Implementacion MCP en este repo:

- [C:\Users\barbo\Documents\PhD\AI Deep Economics\AgroClimaX-mcp-release\agroclimax-mcp\README.md](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX-mcp-release/agroclimax-mcp/README.md)
- [C:\Users\barbo\Documents\PhD\AI Deep Economics\AgroClimaX-mcp-release\agroclimax-mcp\server_app.py](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX-mcp-release/agroclimax-mcp/server_app.py)
- [C:\Users\barbo\Documents\PhD\AI Deep Economics\AgroClimaX-mcp-release\agroclimax-mcp\mcp_server.py](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX-mcp-release/agroclimax-mcp/mcp_server.py)
- [C:\Users\barbo\Documents\PhD\AI Deep Economics\AgroClimaX-mcp-release\apps\backend\app\api\v1\endpoints\integrations_mcp.py](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX-mcp-release/apps/backend/app/api/v1/endpoints/integrations_mcp.py)
- [C:\Users\barbo\Documents\PhD\AI Deep Economics\AgroClimaX-mcp-release\apps\backend\app\services\mcp_integration.py](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX-mcp-release/apps/backend/app/services/mcp_integration.py)
