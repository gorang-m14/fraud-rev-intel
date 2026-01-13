# Fraud & Revenue Intelligence Platform (Postgres + dbt + ClickHouse + Airflow + Streamlit)

A full, local analytics stack that simulates a payments + fraud environment and turns raw OLTP data into fast, dashboard-ready OLAP tables. The project covers the complete path from data generation → transformations/testing → serving layer → dashboards.

---

## What’s inside

- **Postgres (OLTP)**: normalized raw tables (transactions, events, alerts, disputes)
- **dbt**: bronze → silver → gold models + data tests (quality gates)
- **ClickHouse (OLAP)**: serving tables and rollups optimized for analytics
- **Airflow**: pipeline orchestration (ingest → dbt → ClickHouse sync)
- **Streamlit**: clean dashboard UI with filters + merchant drill-down

---

## Architecture (high level)

1. **Ingestion** writes synthetic but realistic payment + event streams into **Postgres**
2. **dbt** transforms raw tables into analytics-ready **gold** models (facts + rollups) with tests
3. **ClickHouse** stores the serving layer (`analytics.*`) for low-latency dashboard queries
4. **Streamlit** reads from ClickHouse to power interactive analytics views
5. **Airflow** schedules and runs the full pipeline

---

## Local UIs

- **Streamlit Dashboard:** http://localhost:8501  
- **Metabase (optional):** http://localhost:3000  
- **Airflow:** http://localhost:8080 (login: `airflow / airflow`)

---

## Quickstart (Docker)

### 0) Prerequisites
- Docker Desktop + Docker Compose

### 1) Start everything
```bash
docker compose up -d --build
