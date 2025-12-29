ã
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "social_data"
TABLE_ID = "wikipedia_daily_views"

def create_table():
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("article_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("views", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE")
    ]
    
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table.clustering_fields = ["category", "article_title"]
    
    try:
        client.create_table(table)
        print(f"Created table {TABLE_ID}")
    except Exception as e:
        print(f"Table might exist: {e}")

if __name__ == "__main__":
    create_table()
ã"(0f1ceee2742f32be6a66898aa01f4fd3b072102f24file:///C:/Users/Yorri/.gemini/setup_views_schema.py:file:///C:/Users/Yorri/.gemini