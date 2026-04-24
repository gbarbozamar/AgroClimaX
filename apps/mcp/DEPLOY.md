# Deploy AgroClimaX MCP Server a Railway

Guía paso a paso para exponer el MCP server como servicio público HTTPS en Railway, consumible desde AgroXpilot / Onyx / Claude Desktop remoto.

## Pre-requisitos

- Cuenta Railway con el proyecto AgroClimaX ya creado (el backend principal ya debe estar deployado).
- URL público del backend (ej. `https://agroclimax-backend-production.up.railway.app`).
- Acceso a las Settings de ese proyecto Railway.

## Paso 1 — Generar service token

En tu máquina local, generá un token aleatorio seguro:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Copiá ese string — lo vas a usar en 3 lugares:
1. Env var `MCP_SERVICE_TOKEN` del backend (Railway)
2. Env var `MCP_SERVICE_TOKEN` del MCP server (Railway)
3. El campo "API Key" de AgroXpilot al configurar el MCP

## Paso 2 — Actualizar el backend (si no está setado aún)

En Railway → proyecto AgroClimaX → service del **backend** → Variables:

| Variable | Valor |
|---|---|
| `MCP_SERVICE_TOKEN` | el token generado en paso 1 |

Restart del service para que tome el env var nuevo.

## Paso 3 — Agregar el MCP server como service Railway

1. En el proyecto Railway, click **+ New** → **GitHub Repo** → seleccionar `AgroClimaX`.
2. Nombre del service: `agroclimax-mcp`.
3. **Settings → General → Root Directory**: `apps/mcp`.
4. Railway detecta automáticamente el `Dockerfile` y `railway.toml`.
5. **Settings → Variables** (o Raw Editor):

```
MCP_SERVICE_TOKEN=<token del paso 1>
AGROCLIMAX_BACKEND_URL=https://<tu-backend-url>.up.railway.app
AGROCLIMAX_MCP_TRANSPORT=http
```

(No hace falta setear `AGROCLIMAX_MCP_PORT` — Railway inyecta `PORT` automáticamente y el Dockerfile lo respeta.)

6. **Settings → Networking → Generate Domain**. Copiá el URL público (ej. `https://agroclimax-mcp-production.up.railway.app`).

## Paso 4 — Deploy

Railway build automático desde el push a main. Ver logs en el tab **Deploy Logs**:

```
INFO:     Started server process [1]
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:<PORT>
```

## Paso 5 — Configurar en AgroXpilot / Onyx

En AgroXpilot → **Admin → Actions → MCP Servers** → editar el server `agroclimax` (o crear uno nuevo):

| Campo | Valor |
|---|---|
| **URL** | `https://agroclimax-mcp-production.up.railway.app/mcp` |
| **Transport** | HTTP (Streamable) |
| **Authentication** | API Key / Shared Key (Admin) |
| **Header Name** | `X-Service-Token` |
| **Header Value** | `{api_key}` (placeholder) |
| **API Key** | `<token del paso 1>` |

Click **Save** + **Sync tools**. Debería listar los **19 tools**.

## Paso 6 — Smoke test

Desde AgroXpilot chat o CLI:

```bash
# Lista los 19 tools (vía Onyx admin API; el MCP server no responde JSON-RPC plano)
# Mejor: usar el chat de Onyx y preguntar algo que invoque un tool:

"¿Cuál es el estado actual de alertas en Uruguay?"
→ invoca get_alert_current
```

O con el smoke_test local apuntando al Railway:

```powershell
$env:AGROCLIMAX_BACKEND_URL = "https://<backend>.up.railway.app"
$env:MCP_SERVICE_TOKEN = "<token>"
python apps\mcp\smoke_test.py
```

## Troubleshooting

**500 en `/api/admin/mcp/server/N/tools/snapshots?source=mcp`**:
- El MCP service no responde. Ver logs de Railway.
- URL mal formado en AgroXpilot (ej. falta `/mcp` al final).

**401 Invalid service token**:
- Token del MCP ≠ token del backend. Ambos deben tener el MISMO valor.

**Tools list devuelve vacío o los viejos (7 en vez de 19)**:
- AgroXpilot cacheó. Click **Sync tools** o eliminá el server y volvé a agregarlo.

**Backend no responde desde el MCP service**:
- `AGROCLIMAX_BACKEND_URL` está con `http://` en vez de `https://`, o apunta a localhost.
- Verificar que el backend del proyecto Railway tenga Networking → Public Domain activo.

## Costos Railway

MCP server es muy liviano (~100 MB RAM idle, picos a ~200 MB durante requests). Plan Hobby de Railway ($5/mes) cubre el backend + MCP sin problema.

## Monitoreo

- Railway → Deploy Logs: stdout del server en vivo.
- El server loggea cada tool invocado. Para auditoría de uso buscar líneas `POST /mcp`.
