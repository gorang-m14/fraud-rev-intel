{{ config(materialized='table') }}

select
  created_at::date as day,
  created_at,
  txn_id,
  customer_id,
  merchant_id,
  payment_method_id,
  amount_cents,
  currency,
  channel,
  status,
  customer_country,
  customer_risk_tier,
  merchant_risk_tier,
  mcc,
  is_large_amount,
  is_bad_outcome
from {{ ref('silver_transactions_enriched') }}
