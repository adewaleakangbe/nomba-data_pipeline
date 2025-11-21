{{ config(materialized='view') }}

select
    txn_id as transaction_key,
    plan_id,
    amount,
    currency,
    side,
    rate,
    txn_timestamp,
    updated_at,
    loaded_at
from {{ source('raw', 'raw_savings_transaction') }}
where deleted_at is null