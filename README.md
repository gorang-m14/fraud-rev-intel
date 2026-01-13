# Fraud & Revenue Intelligence Platform (Postgres + dbt + ClickHouse + Airflow + Streamlit)

An end-to-end analytics stack that simulates a payments/fraud environment and turns raw OLTP events into fast, dashboard-ready OLAP tables.

It includes:
- **Synthetic data generation** (transactions + user events + alerts)
- **Warehouse modeling with dbt** (bronze → silver → gold + tests)
- **Serving layer in ClickHouse** (optimized for analytics & dashboards)
- **Orchestration with Airflow**
- **A clean Streamlit dashboard** for executive + fraud monitoring views

---

## What you can demo in 2 minutes

1. Bring the stack up with Docker  
2. Generate realistic payment + fraud-like data  
3. Run dbt models + tests  
4. Sync analytics tables into ClickHouse  
5. Open the Streamlit dashboard and filter/drill-down by merchant, country, risk tier

---

## Architecture (high level)

**Postgres (OLTP)**  
Stores normalized raw tables like `transactions`, `events`, `alerts` etc. (write-optimized).

**dbt (Warehouse transforms + tests)**  
Builds analytical models:
- Bronze: thin views on raw tables  
- Silver: enriched/typed models (joins, cleaning)  
- Gold: final fact + KPI rollups (`fct_transactions`, `agg_daily_merchant_kpis`)  
Includes data quality checks (unique/not_null/accepted_values).

**ClickHouse (OLAP serving)**  
Materializes fast query tables for dashboards:
- `analytics.fct_transactions`
- `analytics.agg_daily_merchant_kpis`

**Airflow (Orchestration)**  
Pipeline: ingest → dbt build → ClickHouse sync.

**Streamlit (Product-style analytics UI)**  
Executive KPIs + fraud monitoring + merchant drill-downs.

---

## Local UIs

- **Streamlit Dashboard:** http://localhost:8501  
- **Metabase (optional):** http://localhost:3000  
- **Airflow:** http://localhost:8080  (login: `airflow / airflow`)

---

## Quickstart (Docker)

### 0) Prerequisites
- Docker Desktop + Docker Compose

### 1) Start the stack
```bash
docker compose up -d --build
