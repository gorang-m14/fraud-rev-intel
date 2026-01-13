FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingestion /app/ingestion
COPY warehouse /app/warehouse

ENV PYTHONPATH=/app
CMD ["python", "-m", "ingestion.generate", "--rows", "1000"]
