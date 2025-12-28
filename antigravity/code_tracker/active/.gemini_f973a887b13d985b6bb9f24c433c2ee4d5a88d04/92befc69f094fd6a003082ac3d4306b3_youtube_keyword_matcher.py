éimport os
import sys
from google.cloud import bigquery

# Import Matcher from sibling file
# We assume matcher.py is in C:\Users\Yorri\.gemini
sys.path.append('C:\\Users\\Yorri\\.gemini')
try:
    from matcher import KeywordMatcher
except ImportError:
    print("Error: matcher.py not found or failed to import.")
    sys.exit(1)

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

    cases = []
    ids = []
    for u in updates:
        vid = u['video_id']
        keywords = u['keywords'] # List of strings
        # Format for BQ ARRAY literal: ["a", "b"]
        # Need to quote strings and escape quotes if needed
        # Simple implementation:
        formatted = "[" + ", ".join([f"'{k.replace("'", "\\'")}'" for k in keywords]) + "]"
        
        cases.append(f"WHEN video_id = '{vid}' THEN {formatted}")
        ids.append(f"'{vid}'")
        
    ids_str = ",".join(ids)
    
    query = f"""
    UPDATE `{VIDEOS_TABLE}`
    SET matched_keywords = CASE { " ".join(cases) } END
    WHERE video_id IN ({ids_str})
    """
    
    try:
        job = client.query(query)
        job.result()
        print("BigQuery Keyword Update Complete.")
    except Exception as e:
        if "streaming buffer" in str(e).lower():
             print("BigQuery Update Skipped (Streaming Buffer Active).")
        else:
             print(f"BigQuery Update Failed: {e}")

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
é*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0429file:///C:/Users/Yorri/.gemini/youtube_keyword_matcher.py:file:///C:/Users/Yorri/.gemini