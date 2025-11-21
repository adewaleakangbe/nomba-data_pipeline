from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "simple_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_task = BashOperator(
        task_id="print_hello",
        bash_command="echo '🎉 Airflow is working! Now we can fix the database connections.'"
    )