# agroclimax-mcp

FastMCP server que expone el backend AgroClimaX sobre el Model Context Protocol:
field snapshots, timelines, timelapse video jobs, alertas (current/history/forecast),
metadata de fields/paddocks/establishments, métricas de potreros (Fase 5) y
operaciones administrativas como `trigger_backfill`.

## Tools

19 tools disponibles — cubren snapshots, timelines, videos, alertas, metadata de fields/paddocks/establishments, y operaciones administrativas (backfill, layers-available). El MCP server espera que el backend en `AGROCLIMAX_BACKEND_URL` exponga `/api/v1/mcp/...`; algunas tools degradan a `{"status":"not_implemented"}` si la ruta backend aún no está wired.

### Snapshots & timelines

| Tool | Descripción | Params |
|---|---|---|
| `get_field_snapshot` | Snapshot PNG + metadata de un campo en una fecha. | `field_id`, `layer='ndvi'`, `date?`, `user_id?` |
| `get_field_timeline` | Últimos N snapshots del campo. | `field_id`, `layer='ndvi'`, `days=30`, `user_id?` |
| `layers_available` | Capas con snapshots rendereados para un campo. | `field_id`, `user_id?` |
| `download_snapshot_url` | URL directa del PNG de un snapshot (para fetch con header). | `field_id`, `storage_key`, `user_id?` |

### Video timelapse

| Tool | Descripción | Params |
|---|---|---|
| `request_field_video` | Encola job de video timelapse (idempotente sobre jobs recientes). | `field_id`, `layer='ndvi'`, `duration_days=30`, `user_id?` |
| `get_video_status` | Estado de un video job (`queued/rendering/ready/failed` + progress + URL). | `field_id`, `job_id`, `user_id?` |
| `list_video_jobs` | Lista los N video jobs más recientes de un campo. | `field_id`, `limit=20`, `user_id?` |

### Fields / paddocks / establishments metadata

| Tool | Descripción | Params |
|---|---|---|
| `list_user_fields` | Todos los campos de un usuario con metadata básica. | `user_id` |
| `get_field_details` | Detalles completos de un campo incluyendo paddocks. | `field_id`, `user_id?` |
| `list_paddocks` | Potreros activos de un campo. | `field_id`, `user_id?` |
| `list_establishments` | Establecimientos de un usuario. | `user_id` |

### Alerts

| Tool | Descripción | Params |
|---|---|---|
| `list_fields_by_alert` | Campos con alerta >= nivel. | `min_level=2`, `user_id?` |
| `get_alert_current` | Estado actual de alertas según scope. | `scope='nacional'` (`nacional\|departamento\|unidad\|field`), `ref?`, `user_id?` |
| `get_alert_history` | Histórico de alertas (últimos N estados del scope). | `scope='nacional'`, `ref?`, `limit=30`, `user_id?` |
| `get_alert_forecast` | Forecast agroclimático (proyección a N días) si `ForecastSignal` disponible. | `scope='nacional'`, `ref?`, `user_id?` |

### Paddock intelligence (Fase 5)

| Tool | Descripción | Params |
|---|---|---|
| `paddock_metrics` | Métricas agregadas de un potrero (risk current/mean/max 30d, NDMI trend, días en alerta). | `paddock_id`, `date_range_days=30`, `user_id?` |
| `establishment_summary` | Resumen de establecimiento (fields totales, área, highest_risk_field, fields_in_alert). | `establishment_id`, `user_id?` |
| `crop_prediction` | Outlook heurístico de un campo (NDMI trend + risk tier + yield estimate; modelo `heuristic-v0.1`). | `field_id`, `horizon_days=30`, `user_id?` |

### Operaciones administrativas

| Tool | Descripción | Params |
|---|---|---|
| `trigger_backfill` | Dispara backfill async de snapshots para N días y M capas. | `field_id`, `days=30`, `layers?=['ndvi','ndmi','alerta_fusion']`, `user_id?` |

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

- En Claude Desktop, abrí **Settings → Developer**. Deberías ver `agroclimax` listado como un server conectado con sus 19 tools.
- Si aparece en rojo / error, abrí los logs en `%APPDATA%\Claude\logs\mcp-server-agroclimax.log` (macOS: `~/Library/Logs/Claude/mcp-server-agroclimax.log`).
- Probá en el chat: *"usá la tool `list_fields_by_alert` con min_level=2"*. Si el backend está vivo, devuelve JSON; si no, un error de conexión.

### Troubleshooting rápido

| Síntoma | Causa probable |
|---|---|
| Server aparece pero sin tools | `python` en PATH no tiene `fastmcp` instalado. Usá la ruta absoluta al venv. |
| Tools devuelven `Connection refused` | Backend no corriendo en `AGROCLIMAX_BACKEND_URL`. |
| Tools devuelven `401`/`403` | `MCP_SERVICE_TOKEN` no coincide con el del backend. |
| `paddock_metrics` devuelve `{"status": "not_implemented"}` | Endpoint backend aún no wired (otros agentes lo agregan en paralelo). |
