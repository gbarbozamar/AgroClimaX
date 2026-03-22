FROM python:3.12-slim

WORKDIR /app

# Dependencias del sistema (mínimas — sin GDAL)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Dependencias Python
COPY apps/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Backend (server.py + data_fetcher.py + .env.example)
COPY apps/backend/server.py apps/backend/data_fetcher.py ./
COPY apps/backend/.env.example .env.example

# Frontend estático servido por FastAPI StaticFiles
# Copiado a /frontend relativo a la raíz del servicio
COPY apps/frontend/ /frontend/

# Puerto — Railway inyecta $PORT en runtime
EXPOSE 8000

# AgroClimaX: FastAPI + frontend en /
CMD uvicorn server:app --host 0.0.0.0 --port ${PORT:-8000}
