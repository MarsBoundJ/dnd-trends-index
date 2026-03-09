import os
import datetime
from googleapiclient.discovery import build
from google.cloud import bigquery

# Config
API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"
PROJECT_ID = "dnd-trends-index"
VIDEOS_TABLE = f"{PROJECT_ID}.social_data.youtube_videos"

def get_monitored_videos(client):
    # Fetch videos from last 7 days to update stats for
    # We select view_count to calculate velocity
    query = f"""
    SELECT video_id, title, view_count, published_at
    FROM `{VIDEOS_TABLE}`
    WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """
    return list(client.query(query).result())

def batch_update_stats(youtube, video_map):
    # YouTube API allows up to 50 IDs per call
    video_ids = list(video_map.keys())
    updates = []
    
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        ids_str = ",".join(batch)
        
        try:
            req = youtube.videos().list(
                part="statistics",
                id=ids_str
            )
            res = req.execute()
            
            for item in res.get("items", []):
                vid = item["id"]
                stats = item["statistics"]
                current_views = int(stats.get("viewCount", 0))
                
                # Retrieve previous state
                prev_row = video_map.get(vid)
                prev_views = prev_row.view_count if prev_row.view_count else 0
                
                # Calculate Velocity (simple diff)
                # If prev_views is 0 (newly ingested), velocity is current_views (spike)
                velocity = current_views - prev_views
                
                updates.append({
                    "video_id": vid,
                    "view_count": current_views,
                    "velocity_24h": velocity
                })
                
        except Exception as e:
            print(f"Error fetching stats batch: {e}")
            
    return updates

def update_bigquery(client, updates):
    if not updates:
        return

    print(f"Updating {len(updates)} records via MERGE...")
    
    # BigQuery MERGE is more efficient. We'll batch in 100s to be safe.
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        
        # Build the USING clause with SELECT statements
        selects = []
        for u in batch:
            selects.append(f"SELECT '{u['video_id']}' as video_id, {u['view_count']} as vc, {u['velocity_24h']} as vel")
        
        using_sql = " UNION ALL ".join(selects)
        
        query = f"""
        MERGE `{VIDEOS_TABLE}` t
        USING ({using_sql}) s
        ON t.video_id = s.video_id
        WHEN MATCHED THEN
          UPDATE SET view_count = s.vc, velocity_24h = s.vel
        """
        
        try:
            job = client.query(query)
            job.result()
            print(f"Batch {i//batch_size + 1} Complete.")
        except Exception as e:
            print(f"Batch {i//batch_size + 1} Failed: {e}")

def run_updater():
    client = bigquery.Client()
    try:
        youtube = build("youtube", "v3", developerKey=API_KEY)
    except Exception as e:
        print(f"❌ YouTube API Build Failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("Fetching monitored videos from BQ...")
    rows = get_monitored_videos(client)
    print(f"Found {len(rows)} videos to update.")
    
    if not rows:
        return

    # Map for easy lookup
    video_map = {row.video_id: row for row in rows}
    
    print("Fetching latest stats from YouTube...")
    updates = batch_update_stats(youtube, video_map)
    
    if updates:
        print(f"Prepared updates for {len(updates)} videos.")
        print("Sample Update:", updates[0])
        update_bigquery(client, updates)
    else:
        print("No updates found.")

if __name__ == "__main__":
    run_updater()
