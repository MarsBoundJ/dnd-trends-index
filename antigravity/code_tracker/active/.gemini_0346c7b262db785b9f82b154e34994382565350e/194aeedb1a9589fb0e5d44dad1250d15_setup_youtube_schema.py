Ûfrom google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.social_data'

def setup_youtube_registry_schema():
    client = bigquery.Client()
    
    # youtube_channel_registry
    table_id = f'{DATASET_ID}.youtube_channel_registry'
    
    schema = [
        bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("channel_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("handle", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("uploads_playlist_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tier", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("last_scanned_at", "TIMESTAMP", mode="NULLABLE")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Clustering
    table.clustering_fields = ["tier"]

    print(f"Creating/Updating table {table_id}...")
    try:
        table = client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Table {table_id} already exists.")
        else:
            print(f"Error creating table: {e}")

    # youtube_videos
    videos_table_id = f'{DATASET_ID}.youtube_videos'
    videos_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("channel_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tier", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("published_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("view_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("velocity_24h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_pos_ratio", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("matched_keywords", "STRING", mode="REPEATED")
    ]
    
    v_table = bigquery.Table(videos_table_id, schema=videos_schema)
    v_table.clustering_fields = ["published_at"]
    
    print(f"Creating/Updating table {videos_table_id}...")
    try:
        v_table = client.create_table(v_table)
        print(f"Created table {videos_table_id}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Table {videos_table_id} already exists.")
        else:
            print(f"Error creating table: {e}")

if __name__ == "__main__":
    setup_youtube_registry_schema()
Û"(0346c7b262db785b9f82b154e34994382565350e26file:///C:/Users/Yorri/.gemini/setup_youtube_schema.py:file:///C:/Users/Yorri/.gemini