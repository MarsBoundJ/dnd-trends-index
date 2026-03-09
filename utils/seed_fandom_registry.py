import os
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "social_data"
TABLE_ID = "fandom_article_registry"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
SOURCE_DIR = "exported_scraping_work"

def create_registry_table(client):
    schema = [
        bigquery.SchemaField("article_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("wiki_slug", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_file", "STRING", mode="NULLABLE")
    ]
    table = bigquery.Table(FULL_TABLE_ID, schema=schema)
    try:
        client.create_table(table)
        print(f"Created table {FULL_TABLE_ID}")
    except Exception as e:
        print(f"Table exists or error: {e}")

def seed_registry():
    client = bigquery.Client()
    create_registry_table(client)
    
    files = [f for f in os.listdir(SOURCE_DIR) if f.startswith("fandom_") and f.endswith(".txt")]
    print(f"Found {len(files)} list files.")
    
    all_rows = []
    
    for filename in files:
        category = filename.replace("fandom_", "").replace(".txt", "").capitalize()
        filepath = os.path.join(SOURCE_DIR, filename)
        
        with open(filepath, "r") as f:
            lines = f.readlines()
            
        print(f"Processing {filename} ({len(lines)} items)...")
        
        for line in lines:
            title = line.strip()
            if not title: continue
            
            # Basic cleanup: remove (Monster), (Spell) if present?
            # User's list has "Aarakocra (Monster)".
            # We'll keep it as is for now, as that might be the actual page title.
            
            all_rows.append({
                "article_title": title,
                "wiki_slug": "dnd5e", # Defaulting to main wiki
                "category": category,
                "source_file": filename
            })
            
    if all_rows:
        print(f"Inserting {len(all_rows)} rows into BigQuery...")
        # Chunking just in case
        chunk_size = 1000
        for i in range(0, len(all_rows), chunk_size):
            chunk = all_rows[i:i+chunk_size]
            errors = client.insert_rows_json(FULL_TABLE_ID, chunk)
            if errors:
                print(f"Errors in chunk {i}: {errors}")
            else:
                print(f"Inserted chunk {i}")
        print("Done!")

if __name__ == "__main__":
    seed_registry()
