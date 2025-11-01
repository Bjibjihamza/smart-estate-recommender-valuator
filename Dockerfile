# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Africa/Casablanca

# deps for lxml + confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential gcc \
      libxml2-dev libxslt1-dev \
      librdkafka-dev ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps only (code will be mounted at runtime)
RUN pip install --upgrade pip && pip install \
      requests beautifulsoup4 lxml confluent-kafka

# Make /data for CSV output if you need it
RUN mkdir -p /data

# Make src importable at runtime
ENV PYTHONPATH=/app/src

CMD ["python", "-c", "import sys; print('PYTHONPATH:', sys.path)"]
