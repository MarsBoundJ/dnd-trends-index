from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
client = bigquery.Client(project=PROJECT_ID)

def check_status():
    print("--- YT Video Index Status ---")
    query_index = "SELECT transcript_available, count(*) as cnt FROM `dnd-trends-index.dnd_trends_raw.yt_video_index` GROUP BY 1"
    for row in client.query(query_index):
        print(f"Available: {row.transcript_available}, Count: {row.cnt}")

    print("\n--- YT Video Intelligence Status ---")
    query_intel = "SELECT count(*) as cnt FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence`"
    for row in client.query(query_intel):
        print(f"Processed: {row.cnt}")
        
    print("\n--- Pending Processes ---")
    query_pending = """
        SELECT count(*) as cnt
        FROM `dnd-trends-index.dnd_trends_raw.yt_video_index` t
        LEFT JOIN `dnd-trends-index.dnd_trends_raw.yt_video_intelligence` s ON t.video_id = s.video_id
        WHERE t.transcript_available = TRUE AND s.video_id IS NULL
    """
    for row in client.query(query_pending):
        print(f"Pending for Oracle: {row.cnt}")

if __name__ == "__main__":
    check_status()
