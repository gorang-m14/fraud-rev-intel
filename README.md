# Real-Time Fraud + Revenue Intelligence Platform (Postgres + ClickHouse + dbt + Airflow)

This repo is a **resume-grade** SQL + databases project that is **ready to run locally** using Docker.
It simulates a payments marketplace, ingests events into **Postgres (OLTP)**, transforms/validates with **dbt**, and
serves fast analytics tables in **ClickHouse (OLAP)**. **Airflow** orchestrates ingestion + dbt + ClickHouse sync.

> You can start small (Postgres + generator) and then enable Airflow / ClickHouse as you like.

## Architecture (high level)

- **Postgres (OLTP)**: raw normalized tables (`transactions`, `events`, `disputes`, etc.)
- **dbt**: bronze → silver → gold transformations + tests
- **ClickHouse (OLAP)**: analytics serving tables + rollups for dashboards
- **Airflow**: runs ingestion + dbt build + ClickHouse sync on schedule

---

## Quickstart (Docker)

### 0) Prereqs
- Docker + Docker Compose

### 1) Boot everything
```bash
docker compose up -d --build
```

### 2) Generate sample data (OLTP)
This creates synthetic customers/merchants/payment methods and streams transactions + events.
```bash
docker compose run --rm generator python -m ingestion.generate --rows 5000
```

### 3) Run dbt (gold analytics in Postgres)
```bash
docker compose run --rm dbt dbt deps
docker compose run --rm dbt dbt build
```

### 4) Sync to ClickHouse (serving layer)
```bash
docker compose run --rm generator python -m warehouse.sync_to_clickhouse
```

### 5) # Dashboard (Streamlit)
# http://localhost:8501

## What you can say in interviews (talking points)

- Designed OLTP + OLAP separation; implemented **idempotent ingestion** and **dedupe**.
- Built dbt layer with **tests** (uniqueness, not_null, accepted_values) and **freshness** checks.
- Implemented advanced SQL analytics: **cohorts**, **funnels**, **LTV**, **chargeback loss**, **fraud velocity rules**.
- Optimized for performance: Postgres indexes + partitions + ClickHouse partitions/order keys.
- Added governance: PII masking and role-ready patterns.

---

## Useful connections

### Postgres
```bash
docker exec -it fri_postgres psql -U fri -d fri_oltp
```

### ClickHouse
```bash
docker exec -it fri_clickhouse clickhouse-client
```

---

## Repo layout

```
infra/                 # SQL init + docker
ingestion/             # synthetic event generator
warehouse/dbt/         # dbt models/tests (bronze/silver/gold)
warehouse/             # clickhouse sync scripts
orchestration/airflow/ # DAGs
```

---


