‚from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'

def setup_schema():
    client = bigquery.Client()
    
    # 1. reddit_daily_metrics
    # Aggregated volume of specific keywords per day per subreddit
    metrics_table_id = f'{DATASET_ID}.reddit_daily_metrics'
    metrics_schema = [
        bigquery.SchemaField("extraction_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("subreddit", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mention_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("weighted_score", "FLOAT", mode="REQUIRED"), # (upvotes * tier_weight)
    ]
    
    # 2. reddit_viral_events
    # Granular data for individual high-heat posts
    events_table_id = f'{DATASET_ID}.reddit_viral_events'
    events_schema = [
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("subreddit", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("post_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("post_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("upvotes", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE"), # Positive, Negative, etc.
        bigquery.SchemaField("persona", "STRING", mode="NULLABLE"),   # DM vs Player
        bigquery.SchemaField("topic", "STRING", mode="NULLABLE"),     # Main mechanic discussed
    ]
    
    tables = [
        (metrics_table_id, metrics_schema),
        (events_table_id, events_schema)
    ]
    
    for table_id, schema in tables:
        table = bigquery.Table(table_id, schema=schema)
        print(f"Creating table {table_id}...")
        try:
            table = client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        except Exception as e:
            print(f"Table might already exist: {e}")

if __name__ == "__main__":
    setup_schema()
‚"(0f1ceee2742f32be6a66898aa01f4fd3b072102f25file:///C:/Users/Yorri/.gemini/setup_reddit_schema.py:file:///C:/Users/Yorri/.gemini