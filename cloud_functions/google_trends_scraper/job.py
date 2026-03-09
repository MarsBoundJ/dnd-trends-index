import json
import os
import random
import time
import datetime
import uuid
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import bigquery
from watermark import HighWatermark 

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

# Proxy - Use user's Webshare proxy
PROXY_URL = "http://p.webshare.io:9999"

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def fetch_terms_to_process(client, limit=100):
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    query = f"""
        SELECT e.term_id, e.search_term, t.last_date
        FROM `{SOURCE_TABLE}` e
        LEFT JOIN (
            SELECT search_term, MAX(date) as last_date 
            FROM `{DEST_TABLE}`
            GROUP BY search_term
        ) t ON e.search_term = t.search_term
        WHERE e.is_pilot = TRUE 
          AND (t.last_date IS NULL OR t.last_date < '{yesterday}')
        LIMIT {limit}
    """
    return list(client.query(query).result())

def run_job():
    print("🚀 Starting Google Trends Scraper (JOB MODE)...")
    batch_size = int(os.environ.get("BATCH_SIZE", 500))
    
    client = get_bq_client()
    terms = fetch_terms_to_process(client, limit=batch_size)
    print(f"Loaded {len(terms)} terms to process.")
    
    if not terms:
        print("No stale terms found. Skipping.")
        return
        
    batch_id = str(uuid.uuid4())
    proxies_list = [PROXY_URL] * 50
    success_count = 0
    
    for term_row in terms:
        term = term_row.search_term
        print(f"Processing: {term}")
        rows_to_insert = []
        
        try:
            pytrends = TrendReq(hl='en-US', tz=360, retries=2, backoff_factor=1, proxies=proxies_list)
            pytrends.build_payload([term], cat=0, timeframe='today 12-m', geo='', gprop='')
            data = pytrends.interest_over_time()
            
            fetched_at = datetime.datetime.now().isoformat()
            
            if data.empty:
                print(f"  No data for {term}")
                rows_to_insert.append({
                    "term_id": term_row.term_id,
                    "search_term": term,
                    "date": datetime.date.today().isoformat(),
                    "interest": 0,
                    "is_partial": False,
                    "fetched_at": fetched_at,
                    "batch_id": batch_id
                })
            else:
                last_date_str = term_row.last_date
                if last_date_str:
                    last_date = pd.to_datetime(last_date_str).date()
                    filtered_data = data[data.index.date > last_date]
                else:
                    threshold = (datetime.date.today() - datetime.timedelta(days=7))
                    filtered_data = data[data.index.date > threshold]
                
                print(f"  Found {len(filtered_data)} new data points for {term}")
                
                for date_idx, row in filtered_data.iterrows():
                    rows_to_insert.append({
                        "term_id": term_row.term_id,
                        "search_term": term,
                        "date": date_idx.date().isoformat(),
                        "interest": int(row[term]),
                        "is_partial": bool(row.get('isPartial', False)),
                        "fetched_at": fetched_at,
                        "batch_id": batch_id
                    })

            if rows_to_insert:
                errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                if errors:
                    print(f"  BQ Error: {errors}")
                else:
                    success_count += 1
            
            time.sleep(random.uniform(1, 3)) 

        except Exception as e:
            print(f"  Error {term}: {e}")

    print(f"✅ Job Complete. Processed {success_count} terms.")

if __name__ == "__main__":
    run_job()
