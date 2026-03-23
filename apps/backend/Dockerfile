FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY apps/backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/backend/app ./app
COPY apps/backend/tasks ./tasks
COPY apps/backend/data_fetcher.py ./data_fetcher.py
COPY apps/backend/.env.example ./.env.example
COPY evalscripts /evalscripts
COPY apps/frontend /frontend

EXPOSE 8000

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
