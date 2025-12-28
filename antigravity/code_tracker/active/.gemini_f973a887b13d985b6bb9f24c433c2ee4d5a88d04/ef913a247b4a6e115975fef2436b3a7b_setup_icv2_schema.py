”from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'

def setup_icv2_schema():
    client = bigquery.Client()
    
    # icv2_market_reports
    table_id = f'{DATASET_ID}.icv2_market_reports'
    
    schema = [
        bigquery.SchemaField("report_id", "STRING", mode="REQUIRED"), # e.g. "2025_Q1"
        bigquery.SchemaField("period_label", "STRING", mode="REQUIRED"), # "Spring 2025"
        bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("market_sentiment", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_dnd_centric", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("discovered_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Clustering
    table.clustering_fields = ["product_name", "period_label"]

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
    setup_icv2_schema()
”*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0423file:///C:/Users/Yorri/.gemini/setup_icv2_schema.py:file:///C:/Users/Yorri/.gemini