{{ config(materialized='table') }}

select
    user_key,
    user_id,
    first_name,
    last_name,
    occupation,
    state,
    loaded_at as valid_from,
    null as valid_to,
    true as is_current
from {{ ref('stg_users') }}