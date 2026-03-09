import os
import sys
from google.cloud import bigquery

# Import Matcher from sibling file
try:
    from matcher import KeywordMatcher
except ImportError:
    # Fallback if running from a different context
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from matcher import KeywordMatcher

# Config
PROJECT_ID = "dnd-trends-index"
VIDEOS_TABLE = f"{PROJECT_ID}.social_data.youtube_videos"

def get_videos(client):
    query = f"""
    SELECT video_id, title, channel_name
    FROM `{VIDEOS_TABLE}`
    """
    return list(client.query(query).result())

def update_bq_keywords(client, updates):
    if not updates:
        return

    print(f"Updating {len(updates)} video keywords via MERGE...")
    
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        
        selects = []
        for u in batch:
            vid = u['video_id']
            keywords = u['keywords']
            formatted_arr = "[" + ", ".join(["'" + k.replace("'", "\\'") + "'" for k in keywords]) + "]"
            selects.append(f"SELECT '{vid}' as video_id, {formatted_arr} as kw")
        
        using_sql = " UNION ALL ".join(selects)
        
        query = f"""
        MERGE `{VIDEOS_TABLE}` t
        USING ({using_sql}) s
        ON t.video_id = s.video_id
        WHEN MATCHED THEN
          UPDATE SET matched_keywords = s.kw
        """
        
        try:
            job = client.query(query)
            job.result()
            print(f"Batch {i//batch_size + 1} Complete.")
        except Exception as e:
            if "streaming buffer" in str(e).lower():
                 print(f"Batch {i//batch_size + 1} Skipped (Streaming Buffer Active).")
            else:
                 print(f"Batch {i//batch_size + 1} Failed: {e}")

def run_matcher():
    print("Initializing Keyword Matcher...")
    try:
        # Load cache if exists, else build (might take a moment)
        matcher = KeywordMatcher(refresh=False)
    except Exception as e:
        print(f"KeywordMatcher Initialization Failed: {e}")
        return

    client = bigquery.Client()
    videos = get_videos(client)
    print(f"Matching keywords for {len(videos)} videos...")
    
    updates = []
    total_matches = 0
    
    for row in videos:
        # Match against Title
        hits = matcher.find_matches(row.title)
        
        # Extract unique terms
        terms = sorted(list(set([h['term'] for h in hits])))
        
        if terms:
            updates.append({
                "video_id": row.video_id,
                "keywords": terms
            })
            total_matches += len(terms)
            # print(f"[{row.channel_name}] {row.title[:30]}... -> {terms}")

    print(f"Found {total_matches} keyword matches across {len(updates)} videos.")
    
    if updates:
        print("Sample Match:", updates[0])
        update_bq_keywords(client, updates)

if __name__ == "__main__":
    run_matcher()
