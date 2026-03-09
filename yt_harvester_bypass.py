import requests
import xml.etree.ElementTree as ET
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

class RSSHarvester:
    def __init__(self):
        self.bq_client = bigquery.Client()

    def get_active_channels(self):
        query = f"SELECT channel_id, channel_name FROM `{REGISTRY_TABLE}` WHERE active = TRUE"
        return [dict(row) for row in self.bq_client.query(query).result()]

    def get_recent_videos_rss(self, channel_id, limit=10):
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        try:
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                logger.error(f"RSS Failed for {channel_id}: {r.status_code}")
                return []
            
            root = ET.fromstring(r.text)
            ns = {'atom': 'http://www.w3.org/2005/Atom', 'yt': 'http://www.youtube.com/xml/schemas/2015'}
            
            videos = []
            for entry in root.findall('atom:entry', ns)[:limit]:
                video_id = entry.find('yt:videoId', ns).text
                title = entry.find('atom:title', ns).text
                published = entry.find('atom:published', ns).text
                videos.append({
                    "video_id": video_id,
                    "title": title,
                    "published_at": published
                })
            return videos
        except Exception as e:
            logger.error(f"RSS Exception for {channel_id}: {e}")
            return []

    def harvest(self):
        channels = self.get_active_channels()
        total_restored = 0
        
        for ch in channels:
            logger.info(f"Checking {ch['channel_name']}...")
            videos = self.get_recent_videos_rss(ch['channel_id'])
            
            new_records = []
            for vid in videos:
                # Check if exists
                query = f"SELECT count(*) as cnt FROM `{INDEX_TABLE}` WHERE video_id = '{vid['video_id']}'"
                exists = list(self.bq_client.query(query).result())[0].cnt > 0
                
                if exists:
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
                    time.sleep(1) # Gentle throttling
                except Exception as e:
                    logger.warning(f"  Could not get transcript for {vid['video_id']}: {e}")
                    if "sign in" in str(e).lower():
                        logger.warning("  Rate limited. Sleeping 5s.")
                        time.sleep(5)
            
            if new_records:
                self.bq_client.insert_rows_json(INDEX_TABLE, new_records)
                logger.info(f"  ✅ Ingested {len(new_records)} videos.")

        print(f"\nRESTORATION COMPLETE: {total_restored} videos restored.")

if __name__ == "__main__":
    h = RSSHarvester()
    h.harvest()
