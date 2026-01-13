-- OLAP schema (ClickHouse)
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.fct_transactions (
  event_time DateTime64(3, 'UTC'),
  txn_id String,
  customer_id String,
  merchant_id String,
  payment_method_id String,
  amount_cents Int64,
  currency LowCardinality(String),
  channel LowCardinality(String),
  status LowCardinality(String),
  country LowCardinality(String),
  risk_tier LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (merchant_id, event_time, txn_id);

CREATE TABLE IF NOT EXISTS analytics.agg_daily_merchant_kpis (
  day Date,
  merchant_id String,
  gmv_cents Int64,
  net_revenue_cents Int64,
  refund_cents Int64,
  chargeback_cents Int64,
  txn_count UInt64,
  failed_count UInt64,
  high_risk_txn_count UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, merchant_id);
