# Dashboards (optional)

This repo is ready for BI. Recommended options:
- Apache Superset
- Metabase

Quick path:
1) Connect Superset/Metabase to **ClickHouse** (analytics DB) and/or **Postgres** (gold schema).
2) Build pages:
   - Executive overview
   - Growth cohorts
   - Funnel
   - Merchant performance
   - Fraud monitoring
   - Investigations workflow
   - Data health

If you want, I can generate a Superset docker setup + a starter dashboard export based on `analytics.agg_daily_merchant_kpis`.
