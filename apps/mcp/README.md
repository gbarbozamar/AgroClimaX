# agroclimax-mcp

FastMCP server that exposes AgroClimaX backend tools (field snapshots, timelines,
timelapse video jobs, and alert-based field listings) over the Model Context Protocol.

## Tools

- `get_field_snapshot(field_id, layer, date?, user_id?)` - snapshot PNG + metadata.
- `get_field_timeline(field_id, layer, days, user_id?)` - last N snapshots.
- `request_field_video(field_id, layer, duration_days, user_id?)` - enqueue timelapse job.
- `list_fields_by_alert(min_level, user_id?)` - fields with alert >= level.

## Environment

- `AGROCLIMAX_BACKEND_URL` - backend base URL (default `http://127.0.0.1:8001`).
- `MCP_SERVICE_TOKEN` - service token sent as `X-Service-Token`.

## Run locally

```bash
cd apps/mcp
pip install -e .
python server.py
```

Server listens on HTTP at port `8088`.
