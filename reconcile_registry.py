from googleapiclient.discovery import build
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.channel_registry"
YOUTUBE_API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

CREATORS = [
    {"name": "Ginny Di", "handle": "@ginnydi"},
    {"name": "Pointy Hat", "handle": "@PointyHat"},
    {"name": "Master the Dungeon", "handle": "@MasterTheDungeon"},
    {"name": "DnD Shorts", "handle": "@DnDShorts"},
    {"name": "Nerd Immersion", "handle": "@nerdimmersion"},
    {"name": "Treantmonk's Temple", "handle": "@TreantmonksTemple"},
    {"name": "Bob World Builder", "handle": "@BobWorldBuilder"},
    {"name": "Pack Tactics", "handle": "@PackTactics"},
    {"name": "The Dungeon Dudes", "handle": "@DungeonDudes"}
]

def reconcile():
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Clean registry (Optional: depending on if we want to retain history, but for recovery it's better to be clean)
    # Actually, we should just DELETE and RE-SEED correctly.
    
    updates = []
    for c in CREATORS:
        logger.info(f"Resolving {c['name']} ({c['handle']})...")
        try:
            res = youtube.channels().list(part="id,snippet", forHandle=c['handle']).execute()
            if res.get("items"):
                correct_id = res["items"][0]["id"]
                updates.append({
                    "channel_id": correct_id,
                    "channel_name": c['name'],
                    "tier": 1,
                    "active": True
                })
                logger.info(f"  Found ID: {correct_id}")
            else:
                logger.warning(f"  No results for {c['handle']}")
        except Exception as e:
            logger.error(f"  Error resolving {c['handle']}: {e}")

    if updates:
        # Clear old Tier 1
        query = f"DELETE FROM `{REGISTRY_TABLE}` WHERE tier = 1"
        bq_client.query(query).result()
        
        # Insert Corrected
        errors = bq_client.insert_rows_json(REGISTRY_TABLE, updates)
        if not errors:
            logger.info(f"Successfully updated {len(updates)} creators in registry.")
        else:
            logger.error(f"BigQuery Errors: {errors}")

if __name__ == "__main__":
    reconcile()
