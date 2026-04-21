# Field Mode Implementation Status (Fases 2-5)

Consolidated progress for the Field Mode rollout. Fase 1 (multi-level clipping + field scope end-to-end) already landed in commits `70f3b31` and `1a4335b` on `AgroClimaXClaude`. Fases 2-5 are being built in parallel by 13 specialized agents; this document tracks their convergence.

## Progress by phase

| Fase | Descripción | Archivos clave | Estado | Commit |
|------|-------------|----------------|--------|--------|
| 1 | Clipping multi-nivel país/departamento/sección/campo + field scope end-to-end | `apps/backend/app/routers/tiles.py`, `apps/backend/app/services/clipping.py`, `apps/frontend/src/map/scope.ts` | Done | `70f3b31`, `1a4335b` |
| 2 | Field snapshot renderer (layers composited on demand for a single paddock) | `apps/backend/app/services/render_field_snapshot.py`, `apps/backend/tests/test_render_field_snapshot.py` | In progress (tests landed `7fe8074`) | `7fe8074` |
| 3 | Timeline of agronomic events per paddock (siembra, aplicaciones, cosecha) | `apps/backend/app/routers/timeline.py`, `apps/backend/app/models/events.py`, `apps/frontend/src/panels/Timeline.tsx` | In progress (preload plumbing ready) | pending |
| 4 | MCP service surface for Field Mode (read-only tools + auth token) | `apps/mcp/server.py`, `apps/mcp/pyproject.toml`, `apps/mcp/tools/field.py` | In progress (transport + auth shipped, advanced tools stubbed) | pending |
| 5 | Frontend Field Mode UX (sidebar, layer opacity, field deselect, scope reset) | `apps/frontend/src/sidebar/*`, `apps/frontend/src/map/LayerOpacitySlider.tsx` | In progress (sidebar + opacity sliders landed) | `444c8f8`, `a8fb731`, `2175755` |

State legend: Done = merged into `AgroClimaXClaude`; In progress = partial commits landed, work outstanding; Pending = no commit yet.

## Smoke test commands

Validate each phase end-to-end once all commits are in. Replace `$BASE` with the backend base URL (default `http://localhost:8000`) and `$TOKEN` with a valid session token.

```bash
# Fase 1 - multi-level clipping returns a tile clipped to a single paddock
curl -sS "$BASE/tiles/ndvi/2025-04-15/12/3042/2413.png?scope=field&field_id=42" \
     -H "Authorization: Bearer $TOKEN" -o /tmp/f1.png && file /tmp/f1.png

# Fase 2 - field snapshot render for a paddock and date
curl -sS "$BASE/api/fields/42/snapshot?date=2026-04-20&layers=ndvi,ndwi,rain" \
     -H "Authorization: Bearer $TOKEN" | jq '.layers | keys'

# Fase 3 - timeline of agronomic events for a paddock
curl -sS "$BASE/api/fields/42/timeline?from=2025-09-01&to=2026-04-20" \
     -H "Authorization: Bearer $TOKEN" | jq '.events | length'

# Fase 4 - MCP tool list via service token (requires MCP server running)
curl -sS "$MCP_BASE/mcp/tools/list" \
     -H "X-Service-Token: $MCP_SERVICE_TOKEN" | jq '.tools[].name'

# Fase 5 - frontend serves the Field Mode bundle
curl -sS -I "$FRONT_BASE/" | head -1
curl -sS "$FRONT_BASE/assets/field-mode.js" -o /dev/null -w "%{http_code}\n"
```

## Known gaps v1

These items remain stubbed for v1 and will be completed in a follow-up iteration:

- `paddock_metrics` MCP tool - only returns a placeholder payload; real NDVI/NDWI aggregation pipeline pending.
- `establishment_summary` MCP tool - aggregates per-establishment rollups; currently returns a fixed skeleton.
- `crop_prediction` MCP tool - yield/phenology prediction; no model wired, returns `status: "not_implemented"`.
- Paddock overlap tolerance is hard-coded at 15 m (`2175755`); UI-level configuration deferred.
- Timeline events ingestion UI is read-only; authoring flows deferred to Fase 6.
- MCP WebSocket/push updates for live paddock metrics are out of scope for v1.

## How to run the MCP server locally

```bash
cd apps/mcp && pip install -e . && MCP_SERVICE_TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(32))") python server.py
```

Export the printed token into any client that needs to reach the server (`X-Service-Token` header). The server reads `MCP_ALLOWED_HOSTS` for host-allowlist enforcement; set it explicitly when running behind Railway or another proxy.
