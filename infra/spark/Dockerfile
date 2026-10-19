FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/ requirements/
RUN pip install --no-cache-dir -r requirements/spark.txt

# Pre-warm the Ivy cache so containers start without network access.
COPY infra/spark/prewarm.py /tmp/prewarm.py
RUN python /tmp/prewarm.py && rm /tmp/prewarm.py

COPY vimana_common/ vimana_common/
COPY streaming/ streaming/
COPY batch/ batch/
COPY config/ config/
COPY scripts/ scripts/
