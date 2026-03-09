import functions_framework
import requests
import datetime
import json
from google.cloud import bigquery
from datetime import date

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
    url = f"https://{wiki_slug}.fandom.com/api/v1/Articles/Top?expand=1&category=&limit=100"
    
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Error fetching {wiki_slug}: {e}")
        return []

    rows = []
    extraction_date = date.today().isoformat()
    current_rank = 1
    
    for item in data.get('items', []):
        title = item.get('title', '').strip()
        
        # Namespace & Filter
        if any(title.startswith(prefix) for prefix in ["User:", "File:", "Talk:", "Category:", "Template:", "Blog:", "Forum:"]):
            continue
        if title in EXCLUDED_TITLES:
            continue

        hype_score = calculate_hype(current_rank)
        
        row = {
            "extraction_date": extraction_date,
            "wiki_slug": wiki_slug,
            "article_title": title,
            "rank_position": current_rank,
            "hype_score": hype_score,
            "view_count": item.get('views', 0), 
            "url_path": item.get('url', '')
        }
        rows.append(row)
        current_rank += 1
        
    return rows

@functions_framework.http
def fandom_scraper_http(request):
    """
    HTTP Cloud Function Entry Point.
    """
    all_rows = []
    print("🚀 Starting Fandom Harvest (Phase 15)...")
    
    for slug, description in WIKI_REGISTRY.items():
        wiki_rows = fetch_wiki_trends(slug)
        all_rows.extend(wiki_rows)
        
    if all_rows:
        errors = BQ_CLIENT.insert_rows_json(TARGET_TABLE, all_rows)
        if errors:
            print(f"❌ BigQuery Insert Errors: {errors}")
            return json.dumps({"status": "error", "details": str(errors)}), 500
        else:
            msg = f"✅ Success! Inserted {len(all_rows)} rows."
            print(msg)
            return json.dumps({"status": "success", "message": msg}), 200
    else:
        return json.dumps({"status": "warning", "message": "No data harvested"}), 200
