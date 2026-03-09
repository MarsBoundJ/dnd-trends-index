import os
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from googleapiclient.discovery import build
from google.cloud import bigquery

# Config
API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"
PROJECT_ID = "dnd-trends-index"
VIDEOS_TABLE = f"{PROJECT_ID}.social_data.youtube_videos"

def get_videos_for_sentiment(client):
    # Fetch videos from last 7 days that haven't been scored yet (or re-score recently)
    # For MVP, just fetch last 7 days.
    query = f"""
    SELECT video_id, title, channel_name
    FROM `{VIDEOS_TABLE}`
    WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """
    return list(client.query(query).result())

def fetch_and_score_comments(youtube, sia, video_id):
    try:
        req = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=20, # Top 20 relevant comments
            order="relevance",
            textFormat="plainText"
        )
        res = req.execute()
        
        comments = []
        for item in res.get("items", []):
            text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comments.append(text)
            
        if not comments:
            return None

        # Score
        pos_scores = 0
        for c in comments:
            score = sia.polarity_scores(c)
            # Compound > 0.05 is positive
            if score['compound'] > 0.05:
                pos_scores += 1
        
        ratio = pos_scores / len(comments)
        return ratio

    except Exception as e:
        # Comments disabled or other error
        # print(f"Error fetching comments for {video_id}: {e}")
        return None

def update_bq_sentiment(client, updates):
    if not updates:
        return

    print(f"Updating {len(updates)} sentiment scores via MERGE...")
    
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        
        selects = []
        for u in batch:
            selects.append(f"SELECT '{u['video_id']}' as video_id, {u['pos_ratio']} as ratio")
        
        using_sql = " UNION ALL ".join(selects)
        
        query = f"""
        MERGE `{VIDEOS_TABLE}` t
        USING ({using_sql}) s
        ON t.video_id = s.video_id
        WHEN MATCHED THEN
          UPDATE SET sentiment_pos_ratio = s.ratio
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

def run_sentiment_engine():
    print("Initializing Sentiment Engine...")
    try:
        sia = SentimentIntensityAnalyzer()
    except LookupError:
        print("Downloading VADER lexicon...")
        nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()

    client = bigquery.Client()
    youtube = build("youtube", "v3", developerKey=API_KEY)
    
    videos = get_videos_for_sentiment(client)
    print(f"Analyzing sentiment for {len(videos)} videos...")
    
    updates = []
    processed = 0
    
    for row in videos:
        ratio = fetch_and_score_comments(youtube, sia, row.video_id)
        if ratio is not None:
            updates.append({
                "video_id": row.video_id,
                "pos_ratio": round(ratio, 2)
            })
            # print(f"[{row.channel_name}] {row.title[:30]}... -> Pos: {int(ratio*100)}%")
        processed += 1
        if processed % 10 == 0:
            print(f"Processed {processed}/{len(videos)}...")

    print(f"Scored {len(updates)} videos.")
    if updates:
        # Print sample
        print("Sample Score:", updates[0])
        update_bq_sentiment(client, updates)

if __name__ == "__main__":
    run_sentiment_engine()
