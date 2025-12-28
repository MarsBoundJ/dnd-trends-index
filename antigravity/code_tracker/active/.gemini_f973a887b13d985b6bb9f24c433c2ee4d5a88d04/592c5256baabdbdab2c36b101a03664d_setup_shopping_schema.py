Œfrom google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'

def setup_shopping_schema():
    client = bigquery.Client()
    
    # google_shopping_snapshots
    table_id = f'{DATASET_ID}.google_shopping_snapshots'
    
    schema = [
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED", default_value_expression="CURRENT_DATE()"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("retailer", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("discovered_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Clustering
    table.clustering_fields = ["keyword", "retailer"]

    print(f"Creating/Updating table {table_id}...")
    try:
        table = client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Table {table_id} already exists.")
            # Start Update Logic if needed, but for now just pass
        else:
            print(f"Error creating table: {e}")

if __name__ == "__main__":
    setup_shopping_schema()
Œ*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0427file:///C:/Users/Yorri/.gemini/setup_shopping_schema.py:file:///C:/Users/Yorri/.gemini