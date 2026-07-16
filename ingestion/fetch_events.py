import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BASE_URL = "https://api.github.com/events"

def fetch_github_events(per_page=100):
    """Fetch latest public GitHub events."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "github-events-pipeline"
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    response = requests.get(
        BASE_URL,
        headers=headers,
        params={"per_page": per_page}
    )
    response.raise_for_status()
    return response.json()

def parse_event(event):
    """Extract relevant fields from a raw GitHub event."""
    return {
        "event_id": event.get("id"),
        "event_type": event.get("type"),
        "actor": event.get("actor", {}).get("login"),
        "repo_name": event.get("repo", {}).get("name"),
        "created_at": event.get("created_at"),
        "ingested_at": datetime.utcnow().isoformat()
    }

def main():
    print("Fetching GitHub events...")
    raw_events = fetch_github_events()
    parsed = [parse_event(e) for e in raw_events]
    print(f"Fetched {len(parsed)} events")
    for event in parsed[:3]:
        print(json.dumps(event, indent=2))
    return parsed

if __name__ == "__main__":
    main()
