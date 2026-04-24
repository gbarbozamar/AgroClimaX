# AgroClimaX MCP Server — imagen Railway-friendly.
#
# Build context esperado: la carpeta apps/mcp/ del repo.
# Railway monta el proyecto completo, pero con root_directory=apps/mcp
# en railway.toml lo dejamos apuntando acá.
FROM python:3.11-slim

# Evitar .pyc y bufferear stdout/stderr para que Railway vea los logs en vivo.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Instalar solo las dependencias del MCP server (liviano, sin backend).
COPY pyproject.toml ./
RUN pip install --upgrade pip && \
    pip install "fastmcp>=2.0" "httpx>=0.27" "pydantic>=2.0"

# Código del server.
COPY server.py ./
COPY smoke_test.py ./
COPY README.md ./

# Railway inyecta PORT en runtime. Default: 8088 (override via AGROCLIMAX_MCP_PORT).
ENV AGROCLIMAX_MCP_TRANSPORT=http \
    AGROCLIMAX_MCP_PORT=8088

# Exponer el puerto MCP. Railway mapea a su gateway público automáticamente.
EXPOSE 8088

# Start: respetar PORT de Railway si viene seteado (unificado con AGROCLIMAX_MCP_PORT).
CMD ["sh", "-c", "AGROCLIMAX_MCP_PORT=${PORT:-${AGROCLIMAX_MCP_PORT:-8088}} python server.py"]
