{{ config(materialized='view') }}

select
    plan_id as plan_key,
    product_type,
    customer_uid,
    amount as target_amount,
    frequency,
    start_date,
    end_date,
    status,
    created_at,
    updated_at,
    loaded_at
from {{ source('raw', 'raw_savings_plan') }}
where deleted_at is null