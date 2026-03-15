-- This staging model is intentionally simple for the demo.
-- In production, this would pull from the raw (bronze) layer.

select
  account_id,
  account_name,
  account_type,
  account_status,
  customer_id,
  balance_usd,
  currency_code,
  case when trim(last_transaction_date) = '' then null else cast(last_transaction_date as date) end as last_transaction_date,
  case when trim(opened_date) = '' then null else cast(opened_date as date) end as opened_date,
  case when trim(closed_date) = '' then null else cast(closed_date as date) end as closed_date
from {{ ref('accounts') }}
