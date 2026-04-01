import functions_framework
import requests
import json
from collections import Counter
from google.cloud import bigquery
from datetime import date, datetime, timezone, timedelta

# --- CONFIGURATION ---
BQ_CLIENT = bigquery.Client()
TARGET_TABLE = "dnd_trends_raw.fandom_daily_metrics"

# FINALIZED REGISTRY (Phase 15)
WIKI_REGISTRY = {
    "dnd5e": "D&D 5e (Core Mechanics)",
    "forgottenrealms": "Forgotten Realms (Primary Lore)",
    "criticalrole": "Critical Role (Influencer)",
    "dungeonsdragons": "D&D General Lore (Aggregator)",
    "5point5": "D&D 2024 (New Rules)",
    "eberron": "Eberron (Steampunk/Pulp)",
    "ravenloft": "Ravenloft (Horror)",
    "dragonlance": "Dragonlance (High Fantasy)",
    "spelljammer": "Spelljammer (Sci-Fi/Astral)",
    "planescape": "Planescape (Multiverse)",
    "greyhawk": "Greyhawk (Classic/Gygax)",
    "darksun": "Dark Sun (Survival)",
    "mystara": "Mystara (OSR/Retro)"
}

EXCLUDED_TITLE_PREFIXES = ["User:", "File:", "Talk:", "Category:", "Template:", "Blog:", "Forum:"]
EXCLUDED_TITLES = [
    "Main Page", "Wiki_Activity", "Special:Search",
    "Special:Random", "Home", "Dungeons_&_Dragons_Wiki",
    "Community_Corner", "D&D_Wiki"
]

def calculate_hype(rank):
    # Rank 1 = 1.0, Rank 100 = 0.01
    return round(1.01 - (rank * 0.01), 2)

def fetch_wiki_trends(wiki_slug):
    print(f"📡 Scanning {wiki_slug}...")

    # Fandom's /api/v1/Articles/Top is blocked by Cloudflare.
    # Use MediaWiki api.php (not Cloudflare-gated) and rank articles by
    # edit frequency over the past 7 days as the trending signal.
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    params = {
        "action": "query",
        "list": "recentchanges",
        "rclimit": "500",  # MediaWiki max per request
        "rcprop": "title|timestamp",
        "rctype": "edit|new",
        "rcnamespace": "0",  # Main article namespace only
        "rcstart": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rcend": week_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rcdir": "older",
        "format": "json"
    }

    try:
        r = requests.get(
            f"https://{wiki_slug}.fandom.com/api.php",
            params=params,
            timeout=15
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Error fetching {wiki_slug}: {e}")
        return []

    changes = data.get("query", {}).get("recentchanges", [])
    if not changes:
        print(f"⚠️  No recent changes for {wiki_slug}")
        return []

    # Count edits per article, filter noise
    edit_counts = Counter()
    for item in changes:
        title = item.get("title", "").strip()
        if any(title.startswith(p) for p in EXCLUDED_TITLE_PREFIXES):
            continue
        if title in EXCLUDED_TITLES:
            continue
        edit_counts[title] += 1

    ranked = edit_counts.most_common(100)
    extraction_date = date.today().isoformat()
    rows = []

    for rank, (title, count) in enumerate(ranked, start=1):
        rows.append({
            "extraction_date": extraction_date,
            "wiki_slug": wiki_slug,
            "article_title": title,
            "rank_position": rank,
            "hype_score": calculate_hype(rank),
            "view_count": count,  # stores edit_count_7d; schema field kept for compatibility
            "url_path": f"/wiki/{title.replace(' ', '_')}"
        })

    print(f"✅ {wiki_slug}: {len(changes)} edits → {len(rows)} articles ranked")
    return rows


@functions_framework.http
def fandom_scraper_http(request):
    all_rows = []
    print("🚀 Starting Fandom Harvest (MediaWiki recentchanges)...")

    for slug in WIKI_REGISTRY:
        wiki_rows = fetch_wiki_trends(slug)
        all_rows.extend(wiki_rows)

    if all_rows:
        errors = BQ_CLIENT.insert_rows_json(TARGET_TABLE, all_rows)
        if errors:
            print(f"❌ BigQuery Insert Errors: {errors}")
            return json.dumps({"status": "error", "details": str(errors)}), 500
        msg = f"✅ Success! Inserted {len(all_rows)} rows across {len(WIKI_REGISTRY)} wikis."
        print(msg)
        return json.dumps({"status": "success", "message": msg}), 200
    else:
        return json.dumps({"status": "warning", "message": "No data harvested"}), 200
