import os
import logging
from google.cloud import bigquery
from googleapiclient.discovery import build

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.channel_registry"
YOUTUBE_API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

NEW_HANDLES = [
    "@criticalrole", "@vldl_dnd", "@legendsofavantris", "@naddpod", "@WebDM", 
    "@thedmlair", "@mcdm", "@thedungeoncoach", "@wasd20", "@Nerdarchy", 
    "@PackTactics", "@TreantmonksTemple", "@dndbeyond", "@dndwizards", "@AJPickett", 
    "@xp2level3", "@bashewzee", "@zachthebold", "@dungeoncraft", "@DavvyChappy", 
    "@BobWorldBuilder", "@SlyFlourish", "@Taking20", "@Jorphdan", "@PuffinForest", 
    "@DingoDoodles", "@JoCat", "@Runehammer", "@QuestingBeast", "@TabletopNotions", 
    "@Indestructoboy", "@the_aspect", "@dndshorts", "@oneshotquesters", 
    "@dimension20show", "@highrollersdnd", "@oxventure", "@rustage2", "@Koibu0", "@MrRhexx"
]

class RegistryScaler:
    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    def resolve_handle(self, handle):
        """Resolves a @handle to a Channel ID and Name."""
        try:
            res = self.youtube.search().list(
                q=handle,
                part="snippet",
                type="channel",
                maxResults=1
            ).execute()
            
            if not res.get("items"):
                return None
            
            item = res["items"][0]
            return {
                "channel_id": item["snippet"]["channelId"],
                "channel_name": item["snippet"]["channelTitle"]
            }
        except Exception as e:
            logger.error(f"Failed to resolve {handle}: {e}")
            return None

    def scale(self):
        logger.info(f"Scaling registry with {len(NEW_HANDLES)} new handles...")
        
        # 1. Get existing IDs to avoid duplicates
        query = f"SELECT channel_id FROM `{REGISTRY_TABLE}`"
        existing_ids = {row.channel_id for row in self.bq_client.query(query).result()}
        
        rows_to_insert = []
        for handle in NEW_HANDLES:
            data = self.resolve_handle(handle)
            if data and data["channel_id"] not in existing_ids:
                logger.info(f"Adding: {data['channel_name']} ({data['channel_id']})")
                rows_to_insert.append({
                    "channel_id": data["channel_id"],
                    "channel_name": data["channel_name"],
                    "tier": 2, # Default tier for expansion
                    "active": True,
                    "last_scraped": None
                })
                existing_ids.add(data["channel_id"])
            elif data:
                logger.info(f"Already exists: {data['channel_name']}")
            else:
                logger.warning(f"Could not resolve: {handle}")

        if rows_to_insert:
            errors = self.bq_client.insert_rows_json(REGISTRY_TABLE, rows_to_insert)
            if not errors:
                logger.info(f"✅ Successfully added {len(rows_to_insert)} new channels.")
            else:
                logger.error(f"❌ BigQuery Insert Errors: {errors}")
        else:
            logger.info("No new channels to add.")

if __name__ == "__main__":
    scaler = RegistryScaler()
    scaler.scale()
