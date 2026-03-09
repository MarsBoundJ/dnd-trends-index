import functions_framework
import os
import datetime
import json
from googleapiclient.discovery import build
from google.cloud import bigquery
from dateutil import parser as date_parser

# Configuration
# Prefer Environment Variable, fallback to hardcoded (legacy)
API_KEY = os.getenv("YOUTUBE_API_KEY", "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8")
PROJECT_ID = "dnd-trends-index"
REGISTRY_TABLE = f"{PROJECT_ID}.social_data.youtube_channel_registry"
VIDEOS_TABLE = f"{PROJECT_ID}.social_data.youtube_videos"

# Quota Safety
MAX_CHANNELS_PER_RUN = 60 
MAX_VIDEOS_PER_CHANNEL = 5
LOOKBACK_DAYS = 7

def get_channels():
    client = bigquery.Client()
    query = f"""
    SELECT channel_id, channel_name, uploads_playlist_id, tier
    FROM `{REGISTRY_TABLE}`
    ORDER BY tier ASC
    """
    return list(client.query(query).result())

def fetch_recent_uploads(youtube, playlist_id, cutoff_date):
    videos = []
    try:
        req = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=MAX_VIDEOS_PER_CHANNEL
        )
        res = req.execute()
        
        for item in res.get("items", []):
            pub_str = item["snippet"]["publishedAt"]
            pub_dt = date_parser.parse(pub_str)
            
            if pub_dt > cutoff_date:
                videos.append({
                    "video_id": item["contentDetails"]["videoId"],
                    "title": item["snippet"]["title"],
                    "published_at": pub_str,
                    "channel_name": item["snippet"]["channelTitle"] 
                })
    except Exception as e:
        print(f"Error fetching playlist {playlist_id}: {e}")
        
    return videos

@functions_framework.http
def youtube_listener_http(request):
    """
    HTTP entry point for YouTube Social Intelligence Orchestrator.
    """
    print("🚀 Starting YouTube Social Intelligence Orchestration...")
    from youtube_orchestrator import run_orchestration
    
    try:
        run_orchestration()
        return json.dumps({"status": "success", "message": "YouTube Orchestration Complete"}), 200
    except Exception as e:
        print(f"CRITICAL ERROR in Orchestration: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
