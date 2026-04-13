import os
import re
import datetime
from googleapiclient.discovery import build
from google.cloud import bigquery

# Config
API_KEY = os.getenv("YOUTUBE_API_KEY", "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8")
PROJECT_ID = "dnd-trends-index"
VIDEOS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.youtube_videos"

# YouTube Shorts threshold (seconds)
SHORTS_MAX_DURATION = 60


def _parse_iso8601_duration(duration_str):
    """
    Parse ISO 8601 duration (e.g., 'PT1H2M30S', 'PT45S', 'PT3M') to seconds.
    Returns integer seconds, or None if parsing fails.
    """
    if not duration_str:
        return None
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if not match:
        return None
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds

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
                part="statistics,contentDetails",
                id=ids_str
            )
            res = req.execute()

            for item in res.get("items", []):
                vid = item["id"]
                stats = item["statistics"]
                current_views = int(stats.get("viewCount", 0))

                # Extract duration from contentDetails
                content_details = item.get("contentDetails", {})
                duration_iso = content_details.get("duration", "")
                duration_seconds = _parse_iso8601_duration(duration_iso)
                is_short = (duration_seconds is not None and
                            duration_seconds <= SHORTS_MAX_DURATION)

                # Retrieve previous state
                prev_row = video_map.get(vid)
                prev_views = prev_row.view_count if prev_row.view_count else 0

                # Calculate Velocity (simple diff)
                # If prev_views is 0 (newly ingested), velocity is current_views (spike)
                velocity = current_views - prev_views

                updates.append({
                    "video_id": vid,
                    "view_count": current_views,
                    "velocity_24h": velocity,
                    "duration_seconds": duration_seconds,
                    "is_short": is_short,
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
            dur = u['duration_seconds'] if u['duration_seconds'] is not None else 'NULL'
            is_s = 'TRUE' if u['is_short'] else 'FALSE'
            selects.append(
                f"SELECT '{u['video_id']}' as video_id, "
                f"{u['view_count']} as vc, {u['velocity_24h']} as vel, "
                f"{dur} as dur, {is_s} as is_short"
            )

        using_sql = " UNION ALL ".join(selects)

        query = f"""
        MERGE `{VIDEOS_TABLE}` t
        USING ({using_sql}) s
        ON t.video_id = s.video_id
        WHEN MATCHED THEN
          UPDATE SET view_count = s.vc, velocity_24h = s.vel,
                     duration_seconds = s.dur, is_short = s.is_short
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
        shorts_count = sum(1 for u in updates if u.get("is_short"))
        print(f"Prepared updates for {len(updates)} videos ({shorts_count} Shorts detected).")
        print("Sample Update:", updates[0])
        update_bigquery(client, updates)
    else:
        print("No updates found.")

if __name__ == "__main__":
    run_updater()
