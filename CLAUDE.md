# AgroClimaX - Claude Code working notes

Lightweight pointers for Claude Code agents working in this repo. See `docs/` for the full architecture and runbooks.

## Field Mode (Fases 2-5)

Parallel rollout of the Field Mode feature. See `FIELD_MODE_STATUS.md` for the full progress table, smoke tests, and known gaps.

- Fase 1 (landed): multi-level tile clipping and field scope end-to-end. Files: `apps/backend/app/routers/tiles.py`, `apps/backend/app/services/clipping.py`, `apps/frontend/src/map/scope.ts`.
- Fase 2: field snapshot renderer composites layers on demand for a single paddock. Files: `apps/backend/app/services/render_field_snapshot.py`, `apps/backend/tests/test_render_field_snapshot.py`.
- Fase 3: per-paddock timeline of agronomic events (siembra, aplicaciones, cosecha). Files: `apps/backend/app/routers/timeline.py`, `apps/backend/app/models/events.py`, `apps/frontend/src/panels/Timeline.tsx`.
- Fase 4: MCP service surface (read-only tools + `MCP_SERVICE_TOKEN` auth). Files: `apps/mcp/server.py`, `apps/mcp/pyproject.toml`, `apps/mcp/tools/field.py`.
- Fase 5: Field Mode frontend UX (sidebar, layer opacity sliders, field deselect, scope reset). Files: `apps/frontend/src/sidebar/`, `apps/frontend/src/map/LayerOpacitySlider.tsx`.

Full details, smoke tests and `paddock_metrics` / `establishment_summary` / `crop_prediction` stubs are tracked in [`FIELD_MODE_STATUS.md`](./FIELD_MODE_STATUS.md) and [`docs/field_mode.md`](./docs/field_mode.md).
