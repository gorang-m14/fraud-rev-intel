{{ config(materialized='view') }}

select
  txn_id::text as txn_id,
  idempotency_key,
  created_at,
  customer_id::text as customer_id,
  merchant_id::text as merchant_id,
  payment_method_id::text as payment_method_id,
  session_id::text as session_id,
  amount_cents,
  currency,
  channel,
  status,
  auth_code,
  failure_reason
from {{ source('oltp', 'transactions') }}
