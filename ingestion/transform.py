import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def create_transformed_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_github_events (
            event_id VARCHAR(50) PRIMARY KEY,
            event_type VARCHAR(50),
            actor VARCHAR(100),
            repo_name VARCHAR(200),
            repo_owner VARCHAR(100),
            created_at TIMESTAMP,
            ingested_at TIMESTAMP,
            processed_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart_event_summary (
            summary_date DATE,
            event_type VARCHAR(50),
            total_events INT,
            unique_actors INT,
            unique_repos INT,
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (summary_date, event_type)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Transformed tables created")

def run_transformations():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stg_github_events (
            event_id, event_type, actor, repo_name,
            repo_owner, created_at, ingested_at
        )
        SELECT
            event_id,
            event_type,
            actor,
            repo_name,
            SPLIT_PART(repo_name, '/', 1) as repo_owner,
            created_at::timestamp,
            ingested_at::timestamp
        FROM github_events
        ON CONFLICT (event_id) DO NOTHING
    """)
    staged = cur.rowcount
    print(f"Staged {staged} events")
    cur.execute("""
        INSERT INTO mart_event_summary (
            summary_date, event_type,
            total_events, unique_actors, unique_repos
        )
        SELECT
            DATE(created_at) as summary_date,
            event_type,
            COUNT(*) as total_events,
            COUNT(DISTINCT actor) as unique_actors,
            COUNT(DISTINCT repo_name) as unique_repos
        FROM stg_github_events
        GROUP BY DATE(created_at), event_type
        ON CONFLICT (summary_date, event_type)
        DO UPDATE SET
            total_events = EXCLUDED.total_events,
            unique_actors = EXCLUDED.unique_actors,
            unique_repos = EXCLUDED.unique_repos
    """)
    print("Mart table updated")
    conn.commit()
    cur.close()
    conn.close()

def show_results():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT summary_date, event_type,
               total_events, unique_actors, unique_repos
        FROM mart_event_summary
        ORDER BY summary_date DESC, total_events DESC
    """)
    rows = cur.fetchall()
    print("\n=== Event Summary (Mart Table) ===")
    print(f"{'Date':<15} {'Type':<20} {'Events':<10} {'Actors':<10} {'Repos':<10}")
    print("-" * 65)
    for row in rows:
        print(f"{str(row[0]):<15} {row[1]:<20} {row[2]:<10} {row[3]:<10} {row[4]:<10}")
    cur.close()
    conn.close()

def main():
    create_transformed_tables()
    run_transformations()
    show_results()

if __name__ == "__main__":
    main()
