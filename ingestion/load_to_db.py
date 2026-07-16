import psycopg2
import json
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

def create_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS github_events (
            event_id VARCHAR(50) PRIMARY KEY,
            event_type VARCHAR(50),
            actor VARCHAR(100),
            repo_name VARCHAR(200),
            created_at TIMESTAMP,
            ingested_at TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table created successfully")

def load_events(filepath):
    with open(filepath, "r") as f:
        events = json.load(f)

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for event in events:
        try:
            cur.execute("""
                INSERT INTO github_events 
                (event_id, event_type, actor, repo_name, created_at, ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                event["event_id"],
                event["event_type"],
                event["actor"],
                event["repo_name"],
                event["created_at"],
                event["ingested_at"]
            ))
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"Error inserting event {event['event_id']}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted: {inserted} | Skipped (duplicates): {skipped}")

def main():
    create_table()
    # Load the most recent raw file
    raw_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "raw"
    )
    files = sorted(os.listdir(raw_dir))
    if not files:
        print("No raw files found")
        return
    latest = os.path.join(raw_dir, files[-1])
    print(f"Loading: {latest}")
    load_events(latest)

if __name__ == "__main__":
    main()
