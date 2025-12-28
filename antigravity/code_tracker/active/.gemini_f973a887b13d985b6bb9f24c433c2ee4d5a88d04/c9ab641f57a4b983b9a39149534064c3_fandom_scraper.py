¨
import requests
import datetime
import time
from google.cloud import bigquery

# Config
PROJECT_ID = "dnd-trends-index"
TABLE_ID = "social_data.fandom_trending"
WIKI_BASE_URLS = {
    "forgottenrealms": "https://forgottenrealms.fandom.com",
    "dnd5e": "https://dnd5e.fandom.com",
    "criticalrole": "https://criticalrole.fandom.com",
    "eberron": "https://eberron.fandom.com",
    "ravenloft": "https://ravenloft.fandom.com",
    "dragonlance": "https://dragonlance.fandom.com",
    "darksun": "https://darksun.fandom.com",
    "spelljammer": "https://spelljammer.fandom.com",
    "planescape": "https://planescape.fandom.com",
    "greyhawk": "https://greyhawk.fandom.com"
}
USER_AGENT = "DndTrendsIndexBot/1.0 (luckys-story-garden.com)"

def clean_title(title):
    # Remove underscores for display? Or keep raw?
    # Fandom API usually returns readable titles.
    # Namespace cleaning
    bad_namespaces = ["User:", "Talk:", "Category:", "File:", "Template:", "MediaWiki:"]
    for ns in bad_namespaces:
        if title.startswith(ns):
            return None
    
    bad_titles = ["Home", "Wiki_Rules", "Search", "Special:Search", "Special:Random"]
    if title in bad_titles:
        return None
        
    return title

def fetch_trending(wiki_name, base_url):
    endpoint = f"{base_url}/api/v1/Articles/Top"
    params = {
        "limit": 100,
        "expand": 1
    }
    headers = {"User-Agent": USER_AGENT}
    
    print(f"Fetching {wiki_name}...")
    try:
        r = requests.get(endpoint, params=params, headers=headers)
        if r.status_code != 200:
            print(f"Failed {wiki_name}: {r.status_code}")
            return []
            
        data = r.json()
        items = data.get("items", [])
        
        cleaned_items = []
        rank_counter = 1
        
        for item in items:
            title = item.get("title", "")
            if not title:
                continue
            
            clean = clean_title(title)
            if not clean:
                continue
                
            # Fandom doesn't always give rank, reliance on list order
            cleaned_items.append({
                "snapshot_date": datetime.date.today().isoformat(),
                "wiki_name": wiki_name,
                "rank": rank_counter,
                "article_title": clean,
                "article_id": item.get("id"),
                "url_path": item.get("url") 
            })
            rank_counter += 1
            
        return cleaned_items
        
    except Exception as e:
        print(f"Error {wiki_name}: {e}")
        return []

def run_scraper():
    all_rows = []
    
    for wiki, url in WIKI_BASE_URLS.items():
        rows = fetch_trending(wiki, url)
        print(f"  -> Found {len(rows)} valid items for {wiki}")
        all_rows.extend(rows)
        time.sleep(1) # Polite delay between wikis
        
    if all_rows:
        client = bigquery.Client()
        print(f"Pushing {len(all_rows)} rows to BigQuery...")
        errors = client.insert_rows_json(TABLE_ID, all_rows)
        if errors:
            print(f"Errors: {errors}")
        else:
            print("Success!")
    else:
        print("No data found.")

if __name__ == "__main__":
    run_scraper()
Š *cascade08Šº*cascade08º¨ *cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0420file:///C:/Users/Yorri/.gemini/fandom_scraper.py:file:///C:/Users/Yorri/.gemini