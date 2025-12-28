¼from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'

def setup_kickstarter_schema():
    client = bigquery.Client()
    
    # 1. kickstarter_projects
    table_id = f'{DATASET_ID}.kickstarter_projects'
    
    schema = [
        bigquery.SchemaField("project_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("creator", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("backers_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pledged_usd", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("goal_usd", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("percent_funded", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"), # Adventure, Minis, etc.
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"), # live, successful
        bigquery.SchemaField("end_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("is_dnd_centric", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("blurb", "STRING", mode="NULLABLE"), # Useful for context/keyword matching
        bigquery.SchemaField("discovered_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    # Clustering for query performance
    table.clustering_fields = ["category", "is_dnd_centric"]

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
    setup_kickstarter_schema()
¼*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d042:file:///C:/Users/Yorri/.gemini/setup_kickstarter_schema.py:file:///C:/Users/Yorri/.gemini