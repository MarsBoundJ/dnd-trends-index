€$import os
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
    # BQ UPDATE statement
    # We iterate and update (not efficient for massive datasets, but fine for <100 videos)
    # Alternatively, we could dump to a temp table and MERGE. 
    # For <100 items, looping UPDATEs is 'okay' but slow. 
    # Let's use a MERGE via a temp table logic if possible, or just individual updates for MVP transparency.
    # Actually, `client.query` for each might be too slow.
    # Let's simple print for the MVP dry run, then apply.
    
    print(f"Updating {len(updates)} records...")
    
    # Construct a large MERGE query or string
    # For MVP, simpler to just print.
    # But user wants "Engine".
    # Correct approach: Insert new stats into a `stats_updates` temp table, then MERGE.
    
    # Simpler approach for this script:
    # Generate 1 UPDATE query using CASE statement (limit 1MB query size).
    
    if not updates:
        return

    # Build Case Statement
    cases_views = []
    cases_velocity = []
    ids = []
    
    for u in updates:
        vid = u['video_id']
        vc = u['view_count']
        vel = u['velocity_24h']
        
        cases_views.append(f"WHEN video_id = '{vid}' THEN {vc}")
        cases_velocity.append(f"WHEN video_id = '{vid}' THEN {vel}")
        ids.append(f"'{vid}'")
        
    ids_list = ",".join(ids)
    
    query = f"""
    UPDATE `{VIDEOS_TABLE}`
    SET 
        view_count = CASE { " ".join(cases_views) } END,
        velocity_24h = CASE { " ".join(cases_velocity) } END
    WHERE video_id IN ({ids_list})
    """
    
    try:
        job = client.query(query)
        job.result()
        print("BigQuery Update Complete.")
    except Exception as e:
        print(f"BigQuery Update Failed: {e}")

def run_updater():
    client = bigquery.Client()
    youtube = build("youtube", "v3", developerKey=API_KEY)
    
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
€$"(0346c7b262db785b9f82b154e34994382565350e27file:///C:/Users/Yorri/.gemini/youtube_stats_updater.py:file:///C:/Users/Yorri/.gemini