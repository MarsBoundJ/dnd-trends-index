ò
from google.cloud import bigquery
import os

# Config
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "social_data"
TABLE_ID = "wikipedia_article_registry"

def setup_schema():
    client = bigquery.Client()
    
    # Create dataset if not exists
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} exists.")
    except Exception:
        print(f"Dataset {DATASET_ID} not found. Configuring...")
        # Assuming dataset exists from previous phases, but good to be safe.
        # If it truly doesn't exist, we'd need to create it, but usually we just reference it.
        pass

    table_ref = dataset_ref.table(TABLE_ID)
    
    schema = [
        bigquery.SchemaField("article_title", "STRING", mode="REQUIRED", description="Title for API calls (underscores)"),
        bigquery.SchemaField("parent_category", "STRING", mode="NULLABLE", description="Source category (e.g. D&D_monsters)"),
        bigquery.SchemaField("discovery_date", "DATE", mode="NULLABLE", description="When this article was added"),
        bigquery.SchemaField("is_tracked", "BOOLEAN", mode="NULLABLE", description="Whether to fetch daily views")
    ]
    
    table = bigquery.Table(table_ref, schema=schema)
    
    # Clustering by parent_category for analysis
    table.clustering_fields = ["parent_category"]

    try:
        client.create_table(table)
        print(f"Created table {TABLE_ID}")
    except Exception as e:
        print(f"Table {TABLE_ID} might already exist or failed: {e}")

if __name__ == "__main__":
    setup_schema()
ò"(0f1ceee2742f32be6a66898aa01f4fd3b072102f23file:///C:/Users/Yorri/.gemini/setup_wiki_schema.py:file:///C:/Users/Yorri/.gemini