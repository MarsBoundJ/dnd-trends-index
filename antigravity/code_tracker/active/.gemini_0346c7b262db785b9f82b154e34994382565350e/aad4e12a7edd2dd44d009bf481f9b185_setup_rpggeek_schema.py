Ú
from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'

def setup_rpggeek_schema():
    client = bigquery.Client()
    
    # rpggeek_ownership
    table_id = f'{DATASET_ID}.rpggeek_ownership'
    
    schema = [
        bigquery.SchemaField("item_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("owned_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rating", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("is_dnd", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("last_scraped", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"),
        bigquery.SchemaField("rpg_family", "STRING", mode="NULLABLE") # Keeping this as extra metadata
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Clustering
    table.clustering_fields = ["is_dnd"]

    print(f"Creating/Updating table {table_id}...")
    try:
        table = client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        if "already exists" in str(e):
             print(f"Table {table_id} already exists.")
        else:
            print(f"Error creating table: {e}")

if __name__ == "__main__":
    setup_rpggeek_schema()
Ú
"(0346c7b262db785b9f82b154e34994382565350e26file:///C:/Users/Yorri/.gemini/setup_rpggeek_schema.py:file:///C:/Users/Yorri/.gemini