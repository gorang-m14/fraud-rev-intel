FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir dbt-core==1.8.7 dbt-postgres==1.8.2

# If you later want ClickHouse-native dbt models, install dbt-clickhouse here (optional).
# RUN pip install --no-cache-dir dbt-clickhouse==1.8.1

COPY warehouse/dbt /app
ENV DBT_PROFILES_DIR=/app
