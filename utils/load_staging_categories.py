import csv
import os
from google.cloud import bigquery

# Config
CSV_PATH = os.path.join('antigravity', 'scratch', 'dnd_keywords.csv')
TABLE_ID = 'dnd-trends-index.dnd_trends_categorized.staging_categories'

def load_staging():
    client = bigquery.Client()
    
    # Define schema for staging table
    schema = [
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("category", "STRING"),
    ]
    
    # Load data
    rows_to_insert = []
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        return

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_to_insert.append({
                "keyword": row['Keyword'],
                "category": row['Category']
            })
    
    # Create/Overwrite table with schema
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    
    print(f"Loading {len(rows_to_insert)} rows to {TABLE_ID}...")
    job = client.load_table_from_json(rows_to_insert, TABLE_ID, job_config=job_config)
    job.result()
    print("Success!")

if __name__ == "__main__":
    load_staging()
