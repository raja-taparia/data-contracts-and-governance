-- dim_accounts: Data contract-backed dimension model

with base as (
    select *
    from {{ ref('stg_accounts') }}
)

select
    account_id,
    account_name,
    account_type,
    account_status,
    customer_id,
    balance_usd,
    currency_code,
    last_transaction_date,
    opened_date,
    closed_date
from base
