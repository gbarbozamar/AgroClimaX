# agroclimax-mcp

FastMCP server that exposes AgroClimaX backend tools (field snapshots, timelines,
timelapse video jobs, alert-based field listings, y las métricas por potrero de
Fase 5) sobre el Model Context Protocol.

## Tools

- `get_field_snapshot(field_id, layer, date?, user_id?)` — snapshot PNG + metadata.
- `get_field_timeline(field_id, layer, days, user_id?)` — últimos N snapshots.
- `request_field_video(field_id, layer, duration_days, user_id?)` — enqueue timelapse job.
- `list_fields_by_alert(min_level, user_id?)` — campos con alerta >= nivel.
- `paddock_metrics(field_id, user_id?)` — NDVI/NDMI stats por potrero (Fase 5, placeholder hasta que el endpoint backend esté wired).
- `establishment_summary(field_id, user_id?)` — resumen de emergencia / cobertura / stand count (Fase 5, placeholder).
- `crop_prediction(field_id, horizon_days, user_id?)` — predicción de rendimiento / estrés (Fase 5, placeholder).

## Environment

- `AGROCLIMAX_BACKEND_URL` — backend base URL (default `http://127.0.0.1:8001`).
- `MCP_SERVICE_TOKEN` — service token enviado como `X-Service-Token`.
- `AGROCLIMAX_MCP_TRANSPORT` — `stdio` (default, lo que usa Claude Desktop) o `http`.
- `AGROCLIMAX_MCP_PORT` — puerto para transport http (default `8088`).

## Run locally

```bash
cd apps/mcp
pip install -e .
python server.py                      # stdio (Claude Desktop)
AGROCLIMAX_MCP_TRANSPORT=http python server.py   # HTTP en :8088
```

## Conectar con Claude Desktop

Claude Desktop spawnea el server como subproceso por stdio y lee `tools`
automáticamente. El setup es un copy/paste + restart.

### 1. Instalar dependencias

Desde la raíz del repo, una sola vez:

```bash
pip install fastmcp httpx
```

### 2. Pegar la config en `claude_desktop_config.json`

En Windows el archivo vive en `%APPDATA%\Claude\claude_desktop_config.json`
(en macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`).

Si no existe, creá uno. Si existe, mergeá la entrada `mcpServers.agroclimax`.
Hay un template listo en este directorio: [`claude_desktop_config.example.json`](./claude_desktop_config.example.json).

```json
{
  "mcpServers": {
    "agroclimax": {
      "command": "python",
      "args": ["C:/Users/barbo/Documents/PhD/AI Deep Economics/AgroClimaX/.claude/worktrees/nervous-jennings/apps/mcp/server.py"],
      "env": {
        "AGROCLIMAX_BACKEND_URL": "http://127.0.0.1:8001",
        "MCP_SERVICE_TOKEN": "dev-test-token"
      }
    }
  }
}
```

Notas:
- Ajustá la ruta de `args` si movés el repo (usá forward slashes en Windows, Claude Desktop los acepta).
- `MCP_SERVICE_TOKEN` tiene que coincidir con el token que valida `require_service_token` en el backend. Para dev, `dev-test-token` está OK; para producción, rotalo.
- Si usás un virtualenv, reemplazá `"command": "python"` por la ruta absoluta al `python.exe` del venv (ej: `"C:/Users/barbo/.venvs/agroclimax/Scripts/python.exe"`).

### 3. Levantar el backend

Claude Desktop llamará a `http://127.0.0.1:8001`, así que asegurate de tener el backend corriendo:

```bash
cd apps/backend
uvicorn app.main:app --port 8001
```

### 4. Restart Claude Desktop

Cerrá Claude Desktop **completo** (no solo la ventana — revisá el system tray) y volvé a abrirlo. Durante el arranque spawnea cada server listado en `mcpServers` y lee sus tools.

### 5. Verificar que el server cargó

- En Claude Desktop, abrí **Settings → Developer**. Deberías ver `agroclimax` listado como un server conectado con sus 7 tools.
- Si aparece en rojo / error, abrí los logs en `%APPDATA%\Claude\logs\mcp-server-agroclimax.log` (macOS: `~/Library/Logs/Claude/mcp-server-agroclimax.log`).
- Probá en el chat: *"usá la tool `list_fields_by_alert` con min_level=2"*. Si el backend está vivo, devuelve JSON; si no, un error de conexión.

### Troubleshooting rápido

| Síntoma | Causa probable |
|---|---|
| Server aparece pero sin tools | `python` en PATH no tiene `fastmcp` instalado. Usá la ruta absoluta al venv. |
| Tools devuelven `Connection refused` | Backend no corriendo en `AGROCLIMAX_BACKEND_URL`. |
| Tools devuelven `401`/`403` | `MCP_SERVICE_TOKEN` no coincide con el del backend. |
| `paddock_metrics` devuelve `{"status": "not_implemented"}` | Endpoint backend aún no wired (otros agentes lo agregan en paralelo). |
