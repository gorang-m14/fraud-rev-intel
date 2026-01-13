{{ config(materialized='table') }}

with base as (
  select
    day,
    merchant_id,
    sum(case when status in ('authorized','captured') then amount_cents else 0 end) as gmv_cents,
    sum(case when status = 'refunded' then amount_cents else 0 end) as refund_cents,
    sum(case when status = 'chargeback' then amount_cents else 0 end) as chargeback_cents,
    count(*) as txn_count,
    sum(case when status = 'failed' then 1 else 0 end) as failed_count,
    sum(case when customer_risk_tier = 'high' then 1 else 0 end) as high_risk_txn_count
  from {{ ref('fct_transactions') }}
  group by 1,2
)
select
  day,
  merchant_id,
  gmv_cents,
  (gmv_cents - refund_cents - chargeback_cents) as net_revenue_cents,
  refund_cents,
  chargeback_cents,
  txn_count,
  failed_count,
  high_risk_txn_count
from base
