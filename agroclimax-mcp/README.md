# AgroClimaX MCP

Servidor MCP para exponer datos reales de AgroClimaX a un LLM via Model Context Protocol.

## Transporte

- Endpoint principal: `https://mcp.agroclimax.com/mcp`
- Compatibilidad adicional: `https://mcp.agroclimax.com/`
- Healthcheck: `https://mcp.agroclimax.com/healthz`

## Variables de entorno

- `AGROCLIMAX_API_URL`
- `AGROCLIMAX_API_KEY`
- `MCP_SERVER_PORT`
- `MCP_CLIENT_BEARER_TOKENS`
- `INTEGRATION_SERVICE_TOKENS` debe configurarse en el backend principal de AgroClimaX para aceptar el bearer usado por el MCP server.

## Herramientas disponibles

- `find_field(name: str, limit: int = 5)`
- `find_paddock(name: str, field_id: str | None = None, limit: int = 10)`
- `get_field_current_status(field_id: str)`
- `get_field_historical_trend(field_id: str, days: int = 30)`
- `get_paddock_historical_trend(paddock_id: str, days: int = 30)`
- `get_latest_satellite_coverage(field_id: str)`
- `get_active_weather_alerts(field_id: str)`
- `get_paddock_alert_history(paddock_id: str, days: int = 30)`
- `summarize_field_risk(field_id: str)`

## Ejemplos de uso

### Buscar un campo por nombre

Entrada:

```json
{"name": "El Trebol", "limit": 3}
```

Salida estructurada:

```json
{
  "query": "El Trebol",
  "total": 1,
  "items": [
    {
      "id": "farm-field-123",
      "name": "El Trebol",
      "aoi_unit_id": "productive-unit-abc",
      "department": "Rivera",
      "match_score": 1.0
    }
  ]
}
```

### Estado actual de un campo

Entrada:

```json
{"field_id": "farm-field-123"}
```

Salida resumida:

```json
{
  "scope_type": "field",
  "scope_id": "farm-field-123",
  "aoi_unit_id": "productive-unit-abc",
  "selection_label": "El Trebol",
  "status": {
    "state": "Vigilancia",
    "risk_score": 54.1,
    "confidence_score": 69.8
  }
}
```

## Desarrollo local

Instalacion:

```bash
pip install "mcp[cli]"
pip install -r agroclimax-mcp/requirements.txt
```

Run local:

```bash
set AGROCLIMAX_API_URL=http://127.0.0.1:8050
set AGROCLIMAX_API_KEY=tu_service_token
set MCP_CLIENT_BEARER_TOKENS=token-cliente-demo
set MCP_SERVER_PORT=8090
python -m uvicorn server_app:app --app-dir agroclimax-mcp --host 0.0.0.0 --port 8090
```

## Cliente Python

```python
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

async def main():
    headers = {"Authorization": "Bearer token-cliente-demo"}
    async with streamablehttp_client("https://mcp.agroclimax.com/mcp", headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("get_field_current_status", {"field_id": "farm-field-123"})
            print(result)
```

## Deploy Railway

1. Crear un servicio nuevo en Railway apuntando a este repo.
2. Usar `agroclimax-mcp/railway.toml` o configurar manualmente:
   - Dockerfile: `agroclimax-mcp/Dockerfile`
   - healthcheck: `/healthz`
3. Configurar dominio custom `mcp.agroclimax.com`.
4. Cargar las variables de entorno listadas arriba.
