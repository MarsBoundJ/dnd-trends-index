from google.cloud import bigquery
import os

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
INDEX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_index"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_intelligence"

client = bigquery.Client()

print("--- Data Check ---")
total_idx = client.query(f"SELECT count(*) as cnt FROM `{INDEX_TABLE}`").to_dataframe()['cnt'][0]
print(f"Total in Index: {total_idx}")

has_transcript = client.query(f"SELECT count(*) as cnt FROM `{INDEX_TABLE}` WHERE transcript_available = TRUE").to_dataframe()['cnt'][0]
print(f"Has Transcript: {has_transcript}")

in_silver = client.query(f"SELECT count(*) as cnt FROM `{SILVER_TABLE}`").to_dataframe()['cnt'][0]
print(f"Already in Silver: {in_silver}")

query = f"""
SELECT video_id, transcript_available, LENGTH(transcript_text) as t_len
FROM `{INDEX_TABLE}` 
LIMIT 10
"""
data_rows = client.query(query).to_dataframe()
print("--- Video ID Sample ---")
for idx, row in data_rows.iterrows():
    print(f"ID: {row['video_id']} | Available: {row['transcript_available']} | Len: {row['t_len']}")

query_pending = f"""
SELECT t.video_id 
FROM `{INDEX_TABLE}` t
LEFT JOIN `{SILVER_TABLE}` s ON t.video_id = s.video_id
WHERE t.transcript_available = TRUE AND s.video_id IS NULL
LIMIT 5
"""
pending = client.query(query_pending).to_dataframe()
print(f"Pending IDs: {list(pending['video_id'])}")
