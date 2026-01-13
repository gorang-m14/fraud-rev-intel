{{ config(materialized='view') }}

select
  merchant_id::text as merchant_id,
  created_at,
  merchant_name,
  mcc,
  country,
  risk_tier
from {{ source('oltp', 'merchants') }}
