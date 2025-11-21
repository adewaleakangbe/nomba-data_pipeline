-- Simple test model to verify everything works
select 
    'connection_test' as test_name,
    current_timestamp as test_timestamp,
    (select count(*) from raw_users) as user_count,
    (select count(*) from raw_savings_plan) as plan_count,
    (select count(*) from raw_savings_transaction) as transaction_count