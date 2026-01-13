{{ config(materialized='view') }}

select
  customer_id::text as customer_id,
  created_at,
  -- PII masking pattern (example): keep domain, hash local-part
  md5(split_part(email,'@',1)) || '@' || split_part(email,'@',2) as email_masked,
  country,
  risk_tier,
  kyc_tier
from {{ source('oltp', 'customers') }}
