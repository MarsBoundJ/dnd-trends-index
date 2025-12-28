import requests
import json
import time
import datetime
from google.cloud import bigquery

# Config
# Category 34 is Tabletop Games
KICKSTARTER_API_URL = "https://www.kickstarter.com/discover/advanced.json"
DATASET_ID = "dnd-trends-index.commercial_data"
TABLE_ID = f"{DATASET_ID}.kickstarter_projects"

# Task 6.3: Filter Logic
DND_KEYWORDS = [
    "5e", "d&d", "dnd", "5th edition", "dungeons & dragons", 
    "dungeons and dragons", "osr", "2024 compatible", "ttrpg" 
    # TTRPG is broad, but we filter out board games below
]

BOARDGAME_EXCLUSIONS = [
    "board game", "card game", "deck building", "party game", 
    "worker placement", "wargame", "miniatures game" # Distinguish from RPG minis
]

# Task 6.4: Categorization Logic
CATEGORY_MAP = {
    "Adventure": ["adventure", "module", "campaign", "setting", "quest"],
    "Miniatures": ["miniatures", "minis", "stl", "3d print", "sculpt", "terrain"],
    "Accessories": ["dice", "tray", "vault", "screen", "gm screen", "coin", "token"],
    "Player Options": ["class", "subclass", "spell", "sourcebook", "race", "lineage"],
    "Monsters": ["monster", "bestiary", "stat block", "villain", "npc"],
    "Core/System": ["rpg system", "core rulebook", "rule set", "starter set"]
}

import cloudscraper

# ... (rest of imports)

class KickstarterEngine:
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.processed_count = 0
        self.dnd_count = 0
        # Initialize Cloudscraper
        self.scraper = cloudscraper.create_scraper()
        
    def fetch_page(self, page_num):
        params = {
            "category_id": 34, # Tabletop Games
            "sort": "newest",
            "page": page_num,
            "term": "tabletop" 
        }
        # Cloudscraper handles headers internally for the most part, but we can keep Accept
        headers = {
            "Accept": "application/json"
        }
        try:
            print(f"DEBUG: Requesting {KICKSTARTER_API_URL} for page {page_num} via Cloudscraper")
            resp = self.scraper.get(KICKSTARTER_API_URL, params=params, headers=headers)
            print(f"DEBUG: Status Code: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                projects = data.get('projects', [])
                print(f"DEBUG: Found {len(projects)} projects in JSON.")
                if not projects:
                    print(f"DEBUG: Response Dump: {str(data)[:200]}...")
                return projects
            else:
                print(f"Error fetching page {page_num}: {resp.status_code}")
                print(f"Response: {resp.text[:500]}")
                return []
        except Exception as e:
            print(f"Exception fetching page {page_num}: {e}")
            return []

    def classify_project(self, name, blurb):
        text = (str(name) + " " + (str(blurb) or "")).lower()
        
        # 1. D&D Filter
        is_dnd = False
        if any(k in text for k in DND_KEYWORDS):
            is_dnd = True
            
        # Refine: Exclude explicit board games unless they mention 5e
        if any(e in text for e in BOARDGAME_EXCLUSIONS):
            if "5e" not in text and "5th edition" not in text:
                is_dnd = False
                
        # 2. Categorization
        category = "General TTRPG"
        for cat, keywords in CATEGORY_MAP.items():
            if any(k in text for k in keywords):
                category = cat
                break # First match heuristic
                
        return is_dnd, category

    def run_harvest(self, max_pages=10):
        print(f"Starting Kickstarter Harvest (Max Pages: {max_pages})")
        rows_to_insert = []
        
        for p in range(1, max_pages + 1):
            projects = self.fetch_page(p)
            if not projects:
                break
                
            for proj in projects:
                pid = proj.get('id')
                name = proj.get('name')
                blurb = proj.get('blurb')
                creator = proj.get('creator', {}).get('name')
                backers = proj.get('backers_count', 0)
                pledged = proj.get('pledged')
                goal = proj.get('goal')
                state = proj.get('state') # live, successful
                deadline_ts = proj.get('deadline') # Unix timestamp
                url = proj.get('urls', {}).get('web', {}).get('project')
                
                # Logic
                is_dnd, category = self.classify_project(name, blurb)
                
                # Calc funded %
                percent_funded = 0.0
                if goal and goal > 0:
                    percent_funded = round((pledged / goal) * 100, 2)
                
                # Prepare Row
                row = {
                    "project_id": pid,
                    "name": name,
                    "creator": creator,
                    "backers_count": backers,
                    "pledged_usd": float(pledged) if pledged else 0.0,
                    "goal_usd": float(goal) if goal else 0.0,
                    "percent_funded": percent_funded,
                    "category": category,
                    "status": state,
                    "end_date": datetime.datetime.fromtimestamp(deadline_ts).isoformat() if deadline_ts else None,
                    "is_dnd_centric": is_dnd,
                    "blurb": blurb,
                    "discovered_at": datetime.datetime.now().isoformat(),
                    "url": url
                }
                rows_to_insert.append(row)
                
                if is_dnd: self.dnd_count += 1
                self.processed_count += 1
                
            time.sleep(1) # Be polite
            
        # Batch Insert
        if rows_to_insert:
            print(f"Inserting {len(rows_to_insert)} rows to BigQuery...")
            errors = self.bq_client.insert_rows_json(TABLE_ID, rows_to_insert)
            if errors == []:
                print("✅ Data Ingestion Successful.")
            else:
                print(f"❌ Insertion Errors: {errors}")
                
        print(f"Harvest Complete. Processed: {self.processed_count}. D&D Projects: {self.dnd_count}")

if __name__ == "__main__":
    engine = KickstarterEngine()
    engine.run_harvest(max_pages=20) # 20 pages * 20 items = ~400 projects
