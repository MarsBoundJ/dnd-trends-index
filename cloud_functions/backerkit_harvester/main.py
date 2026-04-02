import functions_framework
import requests
import json
import re
from datetime import datetime, timezone, date
from google.cloud import bigquery

# Config
BQ_TABLE = "dnd-trends-index.commercial_data.backerkit_projects"
URL = "https://www.backerkit.com/c/collections/role-playing-games?sort_by=trending"

DND_KEYWORDS = [
    "5e", "5th edition", "d&d", "dungeons", "2024 compatible",
    "black flag", "tales of the valiant", "mcdm", "dragonbane",
]
OSR_KEYWORDS = ["osr", "old school", "old-school", "b/x", "odnd", "ad&d"]


def classify_system(text):
    t = text.lower()
    if any(k in t for k in DND_KEYWORDS):
        return "5e Compatible"
    if any(k in t for k in OSR_KEYWORDS):
        return "OSR"
    return "RPG (Other)"


def parse_amount(amount_str):
    """Parse '$25,385' or '£83,465' → float (numeric value, currency-agnostic)."""
    if not amount_str:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", amount_str)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def days_remaining(ended_at_str):
    """Best-effort parse of BackerKit's ended_at string."""
    try:
        # Format: "April 30, 2026 at 10:00 AM PDT"
        dt = datetime.strptime(ended_at_str.split(" at ")[0], "%B %d, %Y")
        delta = (dt.date() - date.today()).days
        return max(0, delta)
    except Exception:
        return 0


@functions_framework.http
def backerkit_harvester_http(request):
    print("🚀 Starting BackerKit Harvest...")

    try:
        resp = requests.get(
            URL,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120",
                "X-Inertia": "true",
                "Accept": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500

    projects = data.get("crowdfunding/projects", [])
    print(f"Found {len(projects)} projects.")

    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for p in projects:
        title = p.get("title", "")
        text = title + " " + p.get("id", "")
        rows.append({
            "project_id": p.get("id", ""),
            "title": title,
            "creator": p.get("creator_name", ""),
            "funding_usd": parse_amount(p.get("raised_amount", "0")),
            "backers_count": int(p.get("backers", 0)),
            "days_remaining": days_remaining(p.get("ended_at", "")),
            "system_tag": classify_system(text),
            "scraped_at": now,
            "source_url": p.get("formatted_permalink", ""),
        })

    if not rows:
        return json.dumps({"status": "warning", "message": "No projects found"}), 200

    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE, rows)
    if errors:
        print(f"❌ BQ errors: {errors}")
        return json.dumps({"status": "error", "details": str(errors)}), 500

    msg = f"✅ Inserted {len(rows)} BackerKit projects."
    print(msg)
    return json.dumps({"status": "success", "message": msg}), 200
