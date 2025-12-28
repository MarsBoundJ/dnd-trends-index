from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'

def setup_backerkit_schema():
    client = bigquery.Client()
    
    # backerkit_projects
    table_id = f'{DATASET_ID}.backerkit_projects'
    
    schema = [
        bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"), # Slug from URL
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("creator", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("funding_usd", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("backers_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("days_remaining", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("system_tag", "STRING", mode="NULLABLE"), # "5e", "OSR", "New System", etc.
        bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Clustering
    table.clustering_fields = ["system_tag", "project_id"]

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
    setup_backerkit_schema()
