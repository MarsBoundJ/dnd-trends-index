from google.cloud import bigquery
import datetime

# Config
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "commercial_data"
TABLE_ID = "roll20_rankings"

def setup_roll20_schema():
    client = bigquery.Client()
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    
    # Ensure dataset exists
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} exists.")
    except Exception:
        print(f"Dataset {dataset_ref} not found. Creating...")
        client.create_dataset(dataset_ref)
        print(f"Created dataset {dataset_ref}")

    # Define Schema
    table_id = f"{dataset_ref}.{TABLE_ID}"
    schema = [
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="REQUIRED")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    # Cluster by date for efficient historical querying
    table.clustering_fields = ["snapshot_date"]
    
    print(f"Creating/Updating table {table_id}...")
    try:
        table = client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        if "already exists" in str(e):
             print(f"Table {table_id} already exists.")
             # Optional: Update schema if needed (not doing for now to avoid accidental data loss)
        else:
            print(f"Error creating table: {e}")

if __name__ == "__main__":
    setup_roll20_schema()
