FROM python:3.12-slim

WORKDIR /app

COPY agroclimax-mcp/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY agroclimax-mcp/ ./

EXPOSE 8090

CMD ["/bin/sh", "-c", "uvicorn server_app:app --host 0.0.0.0 --port ${MCP_SERVER_PORT:-8090}"]
