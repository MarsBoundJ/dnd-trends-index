import requests
import re
import json
import logging
from google.cloud import bigquery
import datetime
from youtube_transcript_api import YouTubeTranscriptApi
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.channel_registry"
INDEX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_index"

class ScraperHarvester:
    def __init__(self):
        self.bq_client = bigquery.Client()

    def get_active_channels(self):
        query = f"SELECT channel_id, channel_name FROM `{REGISTRY_TABLE}` WHERE active = TRUE"
        return [dict(row) for row in self.bq_client.query(query).result()]

    def get_recent_videos_scraping(self, channel_id, limit=10):
        url = f"https://www.youtube.com/channel/{channel_id}/videos"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        try:
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                logger.error(f"Scrape Failed for {channel_id}: {r.status_code}")
                return []
            
            # Extract ytInitialData blob
            regex = r'var ytInitialData = (\{.*?\});'
            match = re.search(regex, r.text)
            if not match:
                logger.error(f"Could not find ytInitialData for {channel_id}")
                return []
            
            data = json.loads(match.group(1))
            # Navigate to video IDs
            # data['contents']['twoColumnBrowseResultsRenderer']['tabs'][1]['tabRenderer']['content']['richGridRenderer']['contents']
            videos = []
            try:
                tabs = data['contents']['twoColumnBrowseResultsRenderer']['tabs']
                # Usually second tab is 'Videos'
                video_tab = next(t for t in tabs if 'tabRenderer' in t and t['tabRenderer'].get('title') in ['Videos', 'Uploads'])
                contents = video_tab['tabRenderer']['content']['richGridRenderer']['contents']
                
                for item in contents[:limit]:
                    if 'richItemRenderer' in item:
                        video_data = item['richItemRenderer']['content']['videoRenderer']
                        videos.append({
                            "video_id": video_data['videoId'],
                            "title": video_data['title']['runs'][0]['text'],
                            "published_at": datetime.datetime.now().isoformat() # Scraped doesn't have exact timestamp easily
                        })
            except Exception as e:
                logger.error(f"Parsing error for {channel_id}: {e}")
                
            return videos
        except Exception as e:
            logger.error(f"Scrape Exception for {channel_id}: {e}")
            return []

    def harvest(self):
        channels = self.get_active_channels()
        total_restored = 0
        
        for ch in channels:
            logger.info(f"Scraping {ch['channel_name']}...")
            videos = self.get_recent_videos_scraping(ch['channel_id'])
            
            if not videos:
                logger.warning(f"  No videos found for {ch['channel_name']}")
                continue

            new_records = []
            for vid in videos:
                # Check if exists
                query = f"SELECT count(*) as cnt FROM `{INDEX_TABLE}` WHERE video_id = '{vid['video_id']}'"
                exists = list(self.bq_client.query(query).result())[0].cnt > 0
                
                if exists:
                    logger.info(f"  Skipping {vid['video_id']} (Already Indexed)")
                    continue
                
                logger.info(f"  Harvesting: {vid['title']}")
                try:
                    transcript_list = YouTubeTranscriptApi.get_transcript(vid['video_id'])
                    text = " ".join([t['text'] for t in transcript_list])
                    
                    new_records.append({
                        "video_id": vid['video_id'],
                        "channel_id": ch['channel_id'],
                        "title": vid['title'],
                        "published_at": vid['published_at'],
                        "transcript_text": text,
                        "transcript_available": True,
                        "ingestion_date": datetime.date.today().isoformat()
                    })
                    total_restored += 1
                    time.sleep(1)
                    if total_restored >= 10: break # Small batch for Task 3
                except Exception as e:
                    logger.warning(f"  Could not get transcript for {vid['video_id']}: {e}")
            
            if new_records:
                errors = self.bq_client.insert_rows_json(INDEX_TABLE, new_records)
                if not errors:
                    logger.info(f"  ✅ Ingested {len(new_records)} videos.")
                else:
                    logger.error(f"  ❌ BigQuery Errors: {errors}")
            
            if total_restored >= 10: break

        print(f"\nRESTORATION BATCH COMPLETE: {total_restored} videos restored.")

if __name__ == "__main__":
    h = ScraperHarvester()
    h.harvest()
