from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pymongo
from sqlalchemy import create_engine

def test_connections():
    """Simple test to verify all database connections work"""
    print("=== STARTING CONNECTION TEST ===")
    
    try:
        # Test MongoDB
        print("1. Testing MongoDB...")
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
        db = mongo_client["nomba"]
        users_count = db.users.count_documents({})
        print(f"   ✅ MongoDB OK - Found {users_count} users")
    except Exception as e:
        print(f"   ❌ MongoDB Failed: {e}")
        return
    
    try:
        # Test PostgreSQL Source
        print("2. Testing PostgreSQL Source...")
        source_engine = create_engine("postgresql://postgres:password@localhost:5432/nomba_source")
        with source_engine.connect() as conn:
            plans_count = conn.execute("SELECT COUNT(*) FROM savings_plan").scalar()
            transactions_count = conn.execute("SELECT COUNT(*) FROM savingsTransaction").scalar()
            print(f"   ✅ PostgreSQL Source OK - {plans_count} plans, {transactions_count} transactions")
    except Exception as e:
        print(f"   ❌ PostgreSQL Source Failed: {e}")
        return
    
    try:
        # Test PostgreSQL Warehouse
        print("3. Testing PostgreSQL Warehouse...")
        warehouse_engine = create_engine("postgresql://postgres:password@localhost:5432/nomba_warehouse")
        with warehouse_engine.connect() as conn:
            result = conn.execute("SELECT 1")
            print(f"   ✅ PostgreSQL Warehouse OK")
    except Exception as e:
        print(f"   ❌ PostgreSQL Warehouse Failed: {e}")
        return
    
    print("=== ALL CONNECTIONS SUCCESSFUL! ===")

# Create the DAG
with DAG(
    'nomba_simple_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['nomba', 'test']
) as dag:
    
    test_task = PythonOperator(
        task_id='test_all_connections',
        python_callable=test_connections
    )