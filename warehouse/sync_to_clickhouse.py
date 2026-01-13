import os
from datetime import datetime, timezone

import clickhouse_connect
import psycopg2

def pg_dsn():
    return os.environ.get("FRI_PG_DSN", "postgresql://fri:fri@localhost:5432/fri_oltp")

def ch_client():
    host = os.environ.get("FRI_CH_HOST", "localhost")
    port = int(os.environ.get("FRI_CH_HTTP_PORT", "8123"))
    user = os.environ.get("FRI_CH_USER", "fri")
    password = os.environ.get("FRI_CH_PASSWORD", "fri")
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password)

SYNC_SQL = """SELECT
  t.created_at as event_time,
  t.txn_id::text as txn_id,
  t.customer_id::text as customer_id,
  t.merchant_id::text as merchant_id,
  COALESCE(t.payment_method_id::text, '') as payment_method_id,
  t.amount_cents,
  t.currency,
  t.channel,
  t.status,
  c.country,
  c.risk_tier
FROM transactions t
JOIN customers c ON c.customer_id = t.customer_id
WHERE t.created_at >= now() - interval '60 days'
"""

AGG_SQL = """WITH base AS (
  SELECT
    date_trunc('day', t.created_at)::date AS day,
    t.merchant_id::text AS merchant_id,
    SUM(CASE WHEN t.status IN ('authorized','captured') THEN t.amount_cents ELSE 0 END) AS gmv_cents,
    SUM(CASE WHEN t.status = 'refunded' THEN t.amount_cents ELSE 0 END) AS refund_cents,
    SUM(CASE WHEN t.status = 'chargeback' THEN t.amount_cents ELSE 0 END) AS chargeback_cents,
    COUNT(*) AS txn_count,
    SUM(CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN c.risk_tier = 'high' THEN 1 ELSE 0 END) AS high_risk_txn_count
  FROM transactions t
  JOIN customers c ON c.customer_id = t.customer_id
  GROUP BY 1,2
)
SELECT
  day,
  merchant_id,
  gmv_cents,
  (gmv_cents - refund_cents - chargeback_cents) AS net_revenue_cents,
  refund_cents,
  chargeback_cents,
  txn_count::bigint,
  failed_count::bigint,
  high_risk_txn_count::bigint
FROM base
"""

def main():
    # Fetch from Postgres
    pg = psycopg2.connect(pg_dsn())
    cur = pg.cursor()
    cur.execute(SYNC_SQL)
    rows = cur.fetchall()
    print(f"Fetched {len(rows)} txns for ClickHouse sync")
    cur.execute(AGG_SQL)
    agg_rows = cur.fetchall()
    print(f"Fetched {len(agg_rows)} daily merchant KPI rows")

    ch = ch_client()

    # Replace strategy: truncate recent window to keep simple and deterministic
    ch.command("TRUNCATE TABLE analytics.fct_transactions")
    ch.command("TRUNCATE TABLE analytics.agg_daily_merchant_kpis")

    ch.insert(
        "analytics.fct_transactions",
        rows,
        column_names=["event_time","txn_id","customer_id","merchant_id","payment_method_id","amount_cents","currency","channel","status","country","risk_tier"]
    )
    ch.insert(
        "analytics.agg_daily_merchant_kpis",
        agg_rows,
        column_names=["day","merchant_id","gmv_cents","net_revenue_cents","refund_cents","chargeback_cents","txn_count","failed_count","high_risk_txn_count"]
    )
    cur.close()
    pg.close()
    print("ClickHouse sync complete.")

if __name__ == "__main__":
    main()
