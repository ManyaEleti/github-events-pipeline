import json
import os
from datetime import datetime
from fetch_events import fetch_github_events, parse_event

# Always save relative to project root, not current directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def save_to_json(events, output_dir=None):
    if output_dir is None:
        output_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(output_dir, f"github_events_{timestamp}.json")
    with open(filename, "w") as f:
        json.dump(events, f, indent=2)
    print(f"Saved {len(events)} events to {filename}")
    return filename

def main():
    raw_events = fetch_github_events()
    parsed = [parse_event(e) for e in raw_events]
    save_to_json(parsed)

if __name__ == "__main__":
    main()
