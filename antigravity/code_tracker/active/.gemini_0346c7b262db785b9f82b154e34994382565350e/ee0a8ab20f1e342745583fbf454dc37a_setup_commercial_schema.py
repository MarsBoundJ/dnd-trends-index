Ìfrom google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'

def setup_commercial_schema():
    client = bigquery.Client()
    
    # Ensure dataset exists
    dataset_ref = bigquery.Dataset(DATASET_ID)
    dataset_ref.location = "US"
    try:
        client.create_dataset(dataset_ref, timeout=30)
        print(f"Created dataset {DATASET_ID}")
    except Exception:
        print(f"Dataset {DATASET_ID} already exists or error.")

    # 1. dtrpg_velocity
    # Tracks the "Hottest" list rankings over time
    velocity_table_id = f'{DATASET_ID}.dtrpg_velocity'
    velocity_schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("medal_level", "STRING", mode="NULLABLE"), # Gold, Platinum, etc.
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ruleset", "STRING", mode="NULLABLE"), # 5e_Legacy, 5e_2024, OSR
        bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"), # 1-50
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
    ]
    
    # 2. dtrpg_inventory
    # Catalog data to join with keyword library
    inventory_table_id = f'{DATASET_ID}.dtrpg_inventory'
    inventory_schema = [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"), # Full text for keyword mining
        bigquery.SchemaField("last_updated", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("medal_level", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("commercial_weight", "FLOAT", mode="NULLABLE"), # Derived multiplier
    ]
    
    tables = [
        (velocity_table_id, velocity_schema),
        (inventory_table_id, inventory_schema)
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
    setup_commercial_schema()
Ì"(0346c7b262db785b9f82b154e34994382565350e29file:///C:/Users/Yorri/.gemini/setup_commercial_schema.py:file:///C:/Users/Yorri/.gemini