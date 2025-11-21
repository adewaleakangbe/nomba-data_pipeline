from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pymongo
from sqlalchemy import create_engine, text
import logging

default_args = {
    'owner': 'nomba',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def setup_warehouse_tables():
    """Create necessary tables in the data warehouse"""
    try:
        warehouse_engine = create_engine("postgresql://airflow:airflow@postgres:5432/nomba_warehouse")
        
        with warehouse_engine.connect() as conn:
            # Create raw data tables
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw_users (
                    _id TEXT PRIMARY KEY,
                    uid TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    occupation TEXT,
                    state TEXT,
                    loaded_at TIMESTAMP DEFAULT NOW()
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw_savings_plan (
                    plan_id TEXT PRIMARY KEY,
                    product_type TEXT,
                    customer_uid TEXT,
                    amount NUMERIC,
                    frequency TEXT,
                    start_date DATE,
                    end_date DATE,
                    status TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    deleted_at TIMESTAMP,
                    loaded_at TIMESTAMP DEFAULT NOW()
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw_savings_transaction (
                    txn_id TEXT PRIMARY KEY,
                    plan_id TEXT,
                    amount NUMERIC,
                    currency TEXT,
                    side TEXT,
                    rate NUMERIC,
                    txn_timestamp TIMESTAMP,
                    updated_at TIMESTAMP,
                    deleted_at TIMESTAMP,
                    loaded_at TIMESTAMP DEFAULT NOW()
                );
            """))
            
            # Create sync state table for CDC
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    source_name TEXT PRIMARY KEY,
                    last_sync_timestamp TIMESTAMP,
                    last_sync_id TEXT
                );
            """))
            
        logging.info("✅ Warehouse tables created successfully")
        
    except Exception as e:
        logging.error(f"❌ Failed to create warehouse tables: {e}")
        raise

def sync_mongodb_users():
    """Sync MongoDB users with CDC logic (append-only due to no updated_at)"""
    try:
        # Connect to MongoDB
        mongo_client = pymongo.MongoClient("mongodb://host.docker.internal:27017")
        db = mongo_client["nomba"]
        
        # Connect to warehouse
        warehouse_engine = create_engine("postgresql://airflow:airflow@postgres:5432/nomba_warehouse")
        
        # Get last sync state
        with warehouse_engine.connect() as conn:
            result = conn.execute(text("SELECT last_sync_id FROM sync_state WHERE source_name = 'mongodb_users'"))
            last_sync = result.fetchone()
            last_sync_id = last_sync[0] if last_sync else None
        
        # Build query based on last sync
        if last_sync_id:
            # Incremental sync - get records after last sync ID
            query = {"_id": {"$gt": last_sync_id}}
            logging.info(f"🔄 Incremental sync from MongoDB after ID: {last_sync_id}")
        else:
            # Full sync - get all records
            query = {}
            logging.info("🔄 Full sync from MongoDB")
        
        users = db.users.find(query).sort("_id", 1)  # Sort by _id for consistent ordering
        users_processed = 0
        latest_id = last_sync_id
        
        with warehouse_engine.connect() as conn:
            for user in users:
                # Upsert into warehouse
                conn.execute(text("""
                    INSERT INTO raw_users (_id, uid, first_name, last_name, occupation, state)
                    VALUES (:id, :uid, :first_name, :last_name, :occupation, :state)
                    ON CONFLICT (_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    occupation = EXCLUDED.occupation,
                    state = EXCLUDED.state,
                    loaded_at = NOW()
                """), {
                    'id': str(user['_id']),
                    'uid': user.get('Uid', ''),
                    'first_name': user.get('firstName', ''),
                    'last_name': user.get('lastName', ''),
                    'occupation': user.get('occupation', ''),
                    'state': user.get('state', '')
                })
                
                users_processed += 1
                latest_id = str(user['_id'])
            
            # Update sync state
            if latest_id:
                conn.execute(text("""
                    INSERT INTO sync_state (source_name, last_sync_id, last_sync_timestamp)
                    VALUES ('mongodb_users', :last_id, NOW())
                    ON CONFLICT (source_name) DO UPDATE SET
                    last_sync_id = EXCLUDED.last_sync_id,
                    last_sync_timestamp = EXCLUDED.last_sync_timestamp
                """), {'last_id': latest_id})
            
            
        
        logging.info(f"✅ MongoDB Sync: Processed {users_processed} users")
        return f"Processed {users_processed} users"
        
    except Exception as e:
        logging.error(f"❌ MongoDB sync failed: {e}")
        raise

def sync_postgresql_tables():
    """Sync PostgreSQL tables with proper CDC using updated_at"""
    try:
        # Connect to source and warehouse
        source_engine = create_engine("postgresql://postgres:3523@host.docker.internal:5432/nomba_source")
        warehouse_engine = create_engine("postgresql://airflow:airflow@postgres:5432/nomba_warehouse")
        
        # Get last sync timestamp
        with warehouse_engine.connect() as conn:
            result = conn.execute(text("SELECT last_sync_timestamp FROM sync_state WHERE source_name = 'postgresql_tables'"))
            last_sync = result.fetchone()
            last_sync_time = last_sync[0] if last_sync else datetime(1970, 1, 1)  # Very old date if first run
        
        logging.info(f"🔄 PostgreSQL Sync: Last sync was at {last_sync_time}")
        
        plans_processed = 0
        transactions_processed = 0
        
        with source_engine.connect() as source_conn, warehouse_engine.connect() as warehouse_conn:
            # Sync savings_plan table
            plans_query = """
                SELECT plan_id, product_type, customer_uid, amount, frequency, 
                       start_date, end_date, status, created_at, updated_at, deleted_at
                FROM savings_plan 
                WHERE updated_at > :last_sync OR deleted_at IS NOT NULL
            """
            plans = source_conn.execute(text(plans_query), {"last_sync":last_sync_time})
            
            for plan in plans:
                warehouse_conn.execute(text("""
                    INSERT INTO raw_savings_plan 
                    (plan_id, product_type, customer_uid, amount, frequency, start_date, 
                     end_date, status, created_at, updated_at, deleted_at)
                    VALUES (:plan_id, :product_type, :customer_uid, :amount, :frequency, :start_date,
                            :end_date, :status, :created_at, :updated_at, :deleted_at)
                    ON CONFLICT (plan_id) DO UPDATE SET
                    product_type = EXCLUDED.product_type,
                    customer_uid = EXCLUDED.customer_uid,
                    amount = EXCLUDED.amount,
                    frequency = EXCLUDED.frequency,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at,
                    deleted_at = EXCLUDED.deleted_at,
                    loaded_at = NOW()
                """), dict(plan._mapping))
                plans_processed += 1
            
            # Sync savings_transaction table
            transactions_query = """
                SELECT txn_id, plan_id, amount, currency, side, rate, 
                       txn_timestamp, updated_at, deleted_at
                FROM savingsTransaction 
                WHERE updated_at > :last_sync OR deleted_at IS NOT NULL
            """
            transactions = source_conn.execute(text(transactions_query), {"last_sync":last_sync_time})
            
            for transaction in transactions:
                warehouse_conn.execute(text("""
                    INSERT INTO raw_savings_transaction 
                    (txn_id, plan_id, amount, currency, side, rate, txn_timestamp, updated_at, deleted_at)
                    VALUES (:txn_id, :plan_id, :amount, :currency, :side, :rate, 
                            :txn_timestamp, :updated_at, :deleted_at)
                    ON CONFLICT (txn_id) DO UPDATE SET
                    plan_id = EXCLUDED.plan_id,
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    side = EXCLUDED.side,
                    rate = EXCLUDED.rate,
                    txn_timestamp = EXCLUDED.txn_timestamp,
                    updated_at = EXCLUDED.updated_at,
                    deleted_at = EXCLUDED.deleted_at,
                    loaded_at = NOW()
                """), dict(transaction._mapping))
                transactions_processed += 1
            
            # Update sync state
            warehouse_conn.execute(text("""
                INSERT INTO sync_state (source_name, last_sync_timestamp)
                VALUES ('postgresql_tables', NOW())
                ON CONFLICT (source_name) DO UPDATE SET
                last_sync_timestamp = EXCLUDED.last_sync_timestamp
            """))
            
            
        
        logging.info(f"✅ PostgreSQL Sync: Processed {plans_processed} plans and {transactions_processed} transactions")
        return f"Processed {plans_processed} plans and {transactions_processed} transactions"
        
    except Exception as e:
        logging.error(f"❌ PostgreSQL sync failed: {e}")
        raise

def verify_sync():
    """Verify that data was synced correctly"""
    try:
        warehouse_engine = create_engine("postgresql://airflow:airflow@postgres:5432/nomba_warehouse")
        
        with warehouse_engine.connect() as conn:
            # Count records in each table
            users_count = conn.execute(text("SELECT COUNT(*) FROM raw_users")).scalar()
            plans_count = conn.execute(text("SELECT COUNT(*) FROM raw_savings_plan")).scalar()
            transactions_count = conn.execute(text("SELECT COUNT(*) FROM raw_savings_transaction")).scalar()
            
            logging.info(f"📊 Sync Verification:")
            logging.info(f"   Users: {users_count} records")
            logging.info(f"   Plans: {plans_count} records") 
            logging.info(f"   Transactions: {transactions_count} records")
            
            # Check sync state
            sync_state = conn.execute(text("SELECT source_name, last_sync_timestamp FROM sync_state")).fetchall()
            for state in sync_state:
                logging.info(f"   {state[0]}: last sync at {state[1]}")
        
        return "Sync verification completed"
        
    except Exception as e:
        logging.error(f"❌ Sync verification failed: {e}")
        raise

# Create the DAG
with DAG(
    'nomba_cdc_pipeline',
    default_args=default_args,
    description='NOMBA CDC Data Pipeline with MongoDB and PostgreSQL',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['nomba', 'cdc', 'pipeline']
) as dag:
    
    setup_tables = PythonOperator(
        task_id='setup_warehouse_tables',
        python_callable=setup_warehouse_tables
    )
    
    sync_mongodb = PythonOperator(
        task_id='sync_mongodb_users',
        python_callable=sync_mongodb_users
    )
    
    sync_postgresql = PythonOperator(
        task_id='sync_postgresql_tables',
        python_callable=sync_postgresql_tables
    )
    
    verify_sync_task = PythonOperator(
        task_id='verify_sync',
        python_callable=verify_sync
    )
    
    # Set up dependencies
    setup_tables >> [sync_mongodb, sync_postgresql] >> verify_sync_task