import os
import datetime
import logging
from google.cloud import bigquery
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.channel_registry"
INDEX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_index"

# Attempt to load API Key from environment or fallback to found sentinel
YOUTUBE_API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

class TranscriptHarvester:
    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    def get_active_channels(self):
        logger.info("Fetching active channels from registry...")
        query = f"SELECT channel_id, channel_name FROM `{REGISTRY_TABLE}` WHERE active = TRUE"
        return [dict(row) for row in self.bq_client.query(query).result()]

    def get_recent_video_ids(self, channel_id, limit=10):
        """Fetches recent video IDs using the channel's actual 'uploads' playlist."""
        try:
            # 1. Get the uploads playlist ID from channel contentDetails
            ch_res = self.youtube.channels().list(
                part="contentDetails",
                id=channel_id
            ).execute()
            
            if not ch_res.get("items"):
                logger.warning(f"No channel found for ID: {channel_id}")
                return []
                
            uploads_playlist_id = ch_res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            
            # 2. Fetch videos from that playlist
            res = self.youtube.playlistItems().list(
                part="snippet",
                playlistId=uploads_playlist_id,
                maxResults=limit
            ).execute()
            
            videos = []
            for item in res.get("items", []):
                snippet = item["snippet"]
                videos.append({
                    "video_id": snippet["resourceId"]["videoId"],
                    "title": snippet["title"],
                    "published_at": snippet["publishedAt"]
                })
            return videos
            
        except Exception as e:
            logger.error(f"Failed to fetch videos for channel {channel_id}: {e}")
            return []

    def get_transcript(self, video_id):
        """Fetch full transcript text if available with rate-limit sleep and proxy."""
        import time
        import requests
        from dotenv import load_dotenv
        load_dotenv()
        
        proxy_url = os.getenv("PROXY_URL")
        session = requests.Session()
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
        
        try:
            # Use instance-based list() with http_client for compatibility
            api = YouTubeTranscriptApi(http_client=session)
            transcript_list = api.list(video_id)
            transcript = transcript_list.find_transcript(['en', 'en-US']).fetch()
            full_text = " ".join([t.text for t in transcript])
            return full_text, True
        except (TranscriptsDisabled, NoTranscriptFound):
            logger.warning(f"Transcript unavailable for video: {video_id}")
            return None, False
        except Exception as e:
            if "sign in" in str(e).lower() or "blocked" in str(e).lower():
                logger.warning(f"Likely blocked or sign-in error for {video_id}. Sleeping 5s...")
                time.sleep(5)
                # One recursive retry attempt
                try:
                    api = YouTubeTranscriptApi(http_client=session)
                    transcript_list = api.list(video_id)
                    transcript = transcript_list.find_transcript(['en', 'en-US']).fetch()
                    full_text = " ".join([t.text for t in transcript])
                    return full_text, True
                except:
                    return None, False
            logger.error(f"Error fetching transcript for {video_id}: {e}")
            return None, False

    def video_exists(self, video_id):
        """Check if video already indexed in BQ with a successful transcript."""
        query = f"SELECT count(*) as cnt FROM `{INDEX_TABLE}` WHERE video_id = @video_id AND transcript_available = TRUE"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("video_id", "STRING", video_id)]
        )
        result = self.bq_client.query(query, job_config=job_config).to_dataframe()
        return result['cnt'][0] > 0

    def harvest(self, limit_per_channel=5):
        channels = self.get_active_channels()
        logger.info(f"Targeting {len(channels)} channels.")
        
        for ch in channels:
            logger.info(f"Processing {ch['channel_name']} ({ch['channel_id']})...")
            videos = self.get_recent_video_ids(ch['channel_id'], limit=limit_per_channel)
            
            new_records = []
            for vid in videos:
                if self.video_exists(vid['video_id']):
                    logger.info(f" Skipping {vid['video_id']} (Already Indexed)")
                    continue
                
                logger.info(f"  Extracting transcript for: {vid['title']}")
                text, available = self.get_transcript(vid['video_id'])
                
                new_records.append({
                    "video_id": vid['video_id'],
                    "channel_id": ch['channel_id'],
                    "title": vid['title'],
                    "published_at": vid['published_at'],
                    "transcript_text": text,
                    "transcript_available": available,
                    "ingestion_date": datetime.date.today().isoformat()
                })
            
            if new_records:
                errors = self.bq_client.insert_rows_json(INDEX_TABLE, new_records)
                if not errors:
                    logger.info(f"✅ Successfully ingested {len(new_records)} videos for {ch['channel_name']}")
                else:
                    logger.error(f"❌ BigQuery Insert Errors: {errors}")
            else:
                logger.info(f"No new videos to ingest for {ch['channel_name']}")

if __name__ == "__main__":
    harvester = TranscriptHarvester()
    harvester.harvest(limit_per_channel=10)
