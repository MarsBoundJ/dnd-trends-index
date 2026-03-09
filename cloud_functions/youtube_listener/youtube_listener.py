import os
import datetime
from googleapiclient.discovery import build
from google.cloud import bigquery
from dateutil import parser as date_parser

# Configuration
API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"
PROJECT_ID = "dnd-trends-index"
REGISTRY_TABLE = f"{PROJECT_ID}.social_data.youtube_channel_registry"
VIDEOS_TABLE = f"{PROJECT_ID}.social_data.youtube_videos"

# Quota Safety
MAX_CHANNELS_PER_RUN = 60 # Iterate through all ~60 seeded channels
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
        # Cost: 1 unit
        req = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=MAX_VIDEOS_PER_CHANNEL
        )
        res = req.execute()
        
        for item in res.get("items", []):
            pub_str = item["snippet"]["publishedAt"]
            pub_dt = date_parser.parse(pub_str)
            
            # Timezone aware comparison
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

def run_listener():
    print("Starting YouTube Listener...")
    try:
        youtube = build("youtube", "v3", developerKey=API_KEY)
    except Exception as e:
        print(f"API Build Failed: {e}")
        return

    channels = get_channels()
    print(f"Loaded {len(channels)} channels from Registry.")
    
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=LOOKBACK_DAYS)
    new_videos = []
    
    api_hits = 0
    for row in channels:
        try:
             safe_name = row.channel_name.encode('ascii', 'replace').decode('ascii')
             print(f"Scanning Tier {row.tier}: {safe_name}...")
        except:
             print(f"Scanning Tier {row.tier}: [Unicode Name]...")
        
        vids = fetch_recent_uploads(youtube, row.uploads_playlist_id, cutoff_date)
        api_hits += 1
        
        for v in vids:
            # Enrich with registry data for BQ
            v["tier"] = row.tier
            # Metrics placeholders (will be updated by stats script)
            v["view_count"] = 0 
            v["velocity_24h"] = 0
            v["sentiment_pos_ratio"] = None
            v["matched_keywords"] = []
            new_videos.append(v)

    print(f"Scan Complete. Found {len(new_videos)} videos from last {LOOKBACK_DAYS} days.")
    print(f"API Cost: ~{api_hits} units.")

    if new_videos:
        print("--- Sample Videos ---")
        for v in new_videos[:3]:
            # Safe print
            try:
                print(f"[{v['channel_name']}] {v['title']} ({v['published_at']})")
            except:
                pass
        
        # Insert to BigQuery
        client = bigquery.Client()
        try:
            errors = client.insert_rows_json(VIDEOS_TABLE, new_videos)
            if errors:
                print(f"BQ Errors: {errors}")
            else:
                print(f"Successfully inserted {len(new_videos)} videos to {VIDEOS_TABLE}.")
        except Exception as e:
            print(f"BigQuery Insert Failed: {e}")

if __name__ == "__main__":
    run_listener()
