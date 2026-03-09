from google.cloud import bigquery
from google.cloud import logging as gcloud_logging
import json

client = bigquery.Client()
project_id = "dnd-trends-index"

def check_wikipedia():
    print("--- Wikipedia Registry Check ---")
    tables = [
        "dnd-trends-index.social_data.wikipedia_article_registry",
        "dnd-trends-index.dnd_trends_categorized.wikipedia_article_registry"
    ]
    for table in tables:
        try:
            query = f"SELECT count(*) as count FROM `{table}`"
            results = client.query(query, location="US").to_dataframe()
            print(f"Table {table}: {results['count'][0]} rows")
        except Exception as e:
            print(f"Table {table}: Not found or error: {e}")

def seed_amazon():
    print("\n--- Amazon ASIN Seeding ---")
    asin_map = "dnd-trends-index.dnd_trends_categorized.amazon_asin_map"
    core_asins = [
        ("Player''s Handbook", "0786969514"),
        ("Monster Manual", "0786965616"),
        ("Dungeon Master''s Guide", "0786965624"),
        ("Tasha''s Cauldron of Everything", "0786967023"),
        ("Xanathar''s Guide to Everything", "0786966116")
    ]
    
    # Check if table exists
    try:
        client.get_table(asin_map)
        print(f"Table {asin_map} exists.")
    except Exception:
        print(f"Creating table {asin_map}...")
        schema = [
            bigquery.SchemaField("concept_name", "STRING"),
            bigquery.SchemaField("asin", "STRING")
        ]
        table = bigquery.Table(asin_map, schema=schema)
        client.create_table(table, location="US")

    for name, asin in core_asins:
        try:
            query = f"SELECT count(*) as count FROM `{asin_map}` WHERE asin = '{asin}'"
            df = client.query(query, location="US").to_dataframe()
            exists = df['count'][0] > 0
            if not exists:
                print(f"Inserting {name} ({asin})...")
                dml = f"INSERT INTO `{asin_map}` (concept_name, asin) VALUES ('{name}', '{asin}')"
                client.query(dml, location="US").result()
            else:
                print(f"{name} ({asin}) already exists.")
        except Exception as e:
            print(f"Error seeding {asin}: {e}")

def get_google_logs():
    print("\n--- Google Trends Scraper Logs (Last 20) ---")
    log_client = gcloud_logging.Client()
    # Filter for the Cloud Run job logs
    # Note: Adjust resource type if needed (cloud_run_job vs cloud_run_revision)
    FILTER = 'resource.type="cloud_run_job" AND resource.labels.job_name="google-trends-scraper"'
    try:
        entries = log_client.list_entries(filter_=FILTER, order_by=gcloud_logging.DESCENDING, page_size=20)
        for entry in entries:
            print(f"[{entry.timestamp}] {entry.payload if isinstance(entry.payload, str) else json.dumps(entry.payload)}")
    except Exception as e:
        print(f"Error fetching logs: {e}")

if __name__ == "__main__":
    check_wikipedia()
    seed_amazon()
    get_google_logs()
