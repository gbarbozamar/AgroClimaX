FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY agroclimax-tiles/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/backend /app/apps/backend
COPY agroclimax-tiles /app/agroclimax-tiles
COPY evalscripts /app/evalscripts

EXPOSE 8100

WORKDIR /app/agroclimax-tiles

CMD ["uvicorn", "server_app:app", "--host", "0.0.0.0", "--port", "8100"]
