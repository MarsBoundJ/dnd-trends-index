import json
import logging
from google.cloud import bigquery
import datetime
from youtube_transcript_api import YouTubeTranscriptApi
import time
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
INDEX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_index"
JSON_FILE = "treantmonk_videos.json"

class JSONHarvester:
    def __init__(self):
        self.bq_client = bigquery.Client()

    def harvest(self, limit=15):
        if not os.path.exists(JSON_FILE):
            logger.error(f"{JSON_FILE} not found.")
            return

        with open(JSON_FILE, "r") as f:
            videos = json.load(f)

        logger.info(f"Loaded {len(videos)} videos from JSON.")
        total_restored = 0
        
        # Taking the first 'limit' videos
        for vid in videos[:limit]:
            video_id = vid['id']
            title = vid['title']
            
            # Check if exists
            query = f"SELECT count(*) as cnt FROM `{INDEX_TABLE}` WHERE video_id = '{video_id}'"
            exists = list(self.bq_client.query(query).result())[0].cnt > 0
            
            if exists:
                logger.info(f"  Skipping {video_id} (Already Indexed)")
                continue
            
            logger.info(f"  Harvesting: {title}")
            try:
                # Use list() for the version in the container
                transcript_list = YouTubeTranscriptApi.list(video_id)
                transcript = transcript_list.find_transcript(['en', 'en-US']).fetch()
                text = " ".join([t['text'] for t in transcript])
                
                record = {
                    "video_id": video_id,
                    "channel_id": "UCRwqXBAMXWJ5R0y62i8_Wap", # Treantmonk's Temple
                    "title": title,
                    "published_at": datetime.datetime.now().isoformat(),
                    "transcript_text": text,
                    "transcript_available": True,
                    "ingestion_date": datetime.date.today().isoformat()
                }
                
                errors = self.bq_client.insert_rows_json(INDEX_TABLE, [record])
                if not errors:
                    logger.info(f"  ✅ Ingested {video_id}")
                    total_restored += 1
                else:
                    logger.error(f"  ❌ BigQuery Errors: {errors}")
                
                time.sleep(2) # Throttle to avoid rate limits
            except Exception as e:
                logger.warning(f"  Could not get transcript for {video_id}: {e}")

        print(f"\nJSON RESTORATION COMPLETE: {total_restored} videos restored.") or (0)

if __name__ == "__main__":
    h = JSONHarvester()
    h.harvest(limit=15)
