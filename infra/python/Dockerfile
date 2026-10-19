FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/ requirements/
RUN pip install --no-cache-dir -r requirements/ingest.txt -r requirements/backend.txt

COPY vimana_common/ vimana_common/
COPY ingest/ ingest/
COPY backend/ backend/
COPY scripts/ scripts/
COPY config/ config/

