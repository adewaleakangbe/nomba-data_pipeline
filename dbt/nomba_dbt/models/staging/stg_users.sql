{{ config(materialized='view') }}

select
    _id as user_key,
    uid as user_id,
    first_name,
    last_name,
    occupation,
    state,
    loaded_at
from {{ source('raw', 'raw_users') }}