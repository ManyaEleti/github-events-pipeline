from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/Users/manya17/github-events-pipeline/ingestion')

default_args = {
    'owner': 'manya',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_fetch():
    from fetch_events import fetch_github_events, parse_event
    from save_events import save_to_json
    events = fetch_github_events()
    parsed = [parse_event(e) for e in events]
    save_to_json(parsed)
    print(f"Fetched and saved {len(parsed)} events")

def run_load():
    from load_to_db import create_table, load_events
    import glob
    raw_dir = '/Users/manya17/github-events-pipeline/data/raw'
    create_table()
    files = sorted(glob.glob(f"{raw_dir}/*.json"))
    if files:
        load_events(files[-1])

def run_transform():
    from transform import create_transformed_tables, run_transformations, show_results
    create_transformed_tables()
    run_transformations()
    show_results()

def run_quality_check():
    import psycopg2
    import os
    from dotenv import load_dotenv
    load_dotenv('/Users/manya17/github-events-pipeline/.env')
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()
    
    # Check 1: row count
    cur.execute("SELECT COUNT(*) FROM github_events")
    count = cur.fetchone()[0]
    print(f"Row count: {count}")
    assert count > 50, f"QUALITY CHECK FAILED: Only {count} rows, expected > 50"
    
    # Check 2: null actors
    cur.execute("SELECT COUNT(*) FROM github_events WHERE actor IS NULL")
    nulls = cur.fetchone()[0]
    null_pct = (nulls / count) * 100
    print(f"Null actor percentage: {null_pct:.2f}%")
    assert null_pct < 5, f"QUALITY CHECK FAILED: {null_pct:.2f}% null actors"
    
    # Check 3: recent data
    cur.execute("""
        SELECT COUNT(*) FROM github_events 
        WHERE created_at::timestamp > NOW() - INTERVAL '24 hours'
    """)
    recent = cur.fetchone()[0]
    print(f"Events in last 24 hours: {recent}")
    
    cur.close()
    conn.close()
    print("ALL QUALITY CHECKS PASSED")

with DAG(
    dag_id='github_events_pipeline',
    default_args=default_args,
    description='End-to-end GitHub Events pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2026, 7, 16),
    catchup=False,
    tags=['github', 'pipeline', 'data-engineering']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_save_events',
        python_callable=run_fetch
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=run_load
    )

    transform_task = PythonOperator(
        task_id='run_transformations',
        python_callable=run_transform
    )

    quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_quality_check
    )

    fetch_task >> load_task >> transform_task >> quality_task
