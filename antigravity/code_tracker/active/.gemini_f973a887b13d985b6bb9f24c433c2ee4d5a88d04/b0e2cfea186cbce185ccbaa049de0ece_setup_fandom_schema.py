ÿ
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "social_data"
TABLE_ID = "fandom_trending"

def create_table():
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    
    schema = [
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("wiki_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("article_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("article_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("url_path", "STRING", mode="NULLABLE")
    ]
    
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="snapshot_date")
    table.clustering_fields = ["wiki_name", "article_title"]
    
    try:
        client.create_table(table)
        print(f"Created table {TABLE_ID}")
    except Exception as e:
        print(f"Table might exist: {e}")

if __name__ == "__main__":
    create_table()
ÿ*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0425file:///C:/Users/Yorri/.gemini/setup_fandom_schema.py:file:///C:/Users/Yorri/.gemini