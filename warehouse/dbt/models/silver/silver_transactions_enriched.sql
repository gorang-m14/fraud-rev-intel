{{ config(materialized='view') }}

with t as (
  select * from {{ ref('bronze_transactions') }}
),
c as (
  select customer_id, country as customer_country, risk_tier as customer_risk_tier, kyc_tier
  from {{ ref('bronze_customers') }}
),
m as (
  select merchant_id, mcc, country as merchant_country, risk_tier as merchant_risk_tier
  from {{ ref('bronze_merchants') }}
)
select
  t.*,
  c.customer_country,
  c.customer_risk_tier,
  c.kyc_tier,
  m.mcc,
  m.merchant_country,
  m.merchant_risk_tier,
  -- example derived flags
  (t.amount_cents >= 150000)::boolean as is_large_amount,
  (t.status in ('failed','chargeback'))::boolean as is_bad_outcome
from t
join c on c.customer_id = t.customer_id
join m on m.merchant_id = t.merchant_id
