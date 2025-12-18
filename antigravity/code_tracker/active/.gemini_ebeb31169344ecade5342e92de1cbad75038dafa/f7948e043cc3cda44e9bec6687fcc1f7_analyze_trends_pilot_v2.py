¨(
import os
import time
import random
import datetime
import uuid
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

BATCH_SIZE = 5
PAUSE_SECONDS = 15

# Pool of static proxies to rotate through (simulating rotation)
PROXY_POOL = [
    "http://oxsjenoi-residential-1:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-2:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-3:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-4:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-5:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-6:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-7:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-8:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-9:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-10:yw72fdfu37vt@p.webshare.io:80",
]
proxy_index = 0

client = bigquery.Client(project=PROJECT_ID)

def get_next_proxy():
    """Cycles through the proxy pool."""
    global proxy_index
    proxy = PROXY_POOL[proxy_index % len(PROXY_POOL)]
    proxy_index += 1
    return proxy

def fetch_unprocessed_terms():
    """
    Fetch only the terms we haven't successfully processed yet.
    Joins expanded_search_terms with trend_data_pilot to find gaps.
    """
    query = f"""
        SELECT e.term_id, e.search_term 
        FROM `{SOURCE_TABLE}` e
        LEFT JOIN (
            SELECT DISTINCT search_term FROM `{DEST_TABLE}`
        ) t ON e.search_term = t.search_term
        WHERE e.is_pilot = TRUE AND t.search_term IS NULL
    """
    return list(client.query(query).result())

def main():
    print("Starting Trend Analysis Pilot v2 (Rotating Proxies)...")
    
    # 1. Fetch ONLY unprocessed terms
    terms = fetch_unprocessed_terms()
    print(f"Loaded {len(terms)} REMAINING terms to process.")
    
    if len(terms) == 0:
        print("All terms already processed! Exiting.")
        return
    
    batch_id = str(uuid.uuid4())
    all_term_details = {row['search_term']: row['term_id'] for row in terms}
    all_terms_list = list(all_term_details.keys())
    
    success_count = 0
    fail_count = 0
    
    for i in range(0, len(all_terms_list), BATCH_SIZE):
        batch_terms = all_terms_list[i : i + BATCH_SIZE]
        
        # Get a fresh proxy for this batch
        current_proxy = get_next_proxy()
        proxies_list = [current_proxy]
        
        print(f"Batch {i//BATCH_SIZE + 1}/{len(all_terms_list)//BATCH_SIZE + 1}: {batch_terms[:2]}... (Proxy: {current_proxy.split('@')[1]})")
        
        try:
            # Re-init pytrends with new proxy each batch
            pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.5, proxies=proxies_list)
            pytrends.build_payload(batch_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
            data = pytrends.interest_over_time()
            
            if data.empty:
                print("  No data returned.")
                continue
                
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                is_partial = bool(row.get('isPartial', False)) if 'isPartial' in data.columns else False

                for term in batch_terms:
                    if term in data.columns:
                        rows_to_insert.append({
                            "term_id": all_term_details.get(term),
                            "search_term": term,
                            "date": date_str,
                            "interest": int(row[term]),
                            "is_partial": is_partial,
                            "fetched_at": fetched_at,
                            "batch_id": batch_id
                        })
            
            if rows_to_insert:
                errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                if errors:
                    print(f"  BQ Error: {errors}")
                else:
                    print(f"  SUCCESS: {len(rows_to_insert)} rows.")
                    success_count += 1
            
        except Exception as e:
            print(f"  FAILED: {str(e)[:80]}...")
            fail_count += 1
            if "429" in str(e):
                print("  Rate limited. Sleeping 60s...")
                time.sleep(60)
        
        # Rate Limiting with jitter
        sleep_time = PAUSE_SECONDS + random.uniform(2, 8)
        time.sleep(sleep_time)

    print(f"\nPilot Run Complete. Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    main()
¨(*cascade08"(ebeb31169344ecade5342e92de1cbad75038dafa29file:///C:/Users/Yorri/.gemini/analyze_trends_pilot_v2.py:file:///C:/Users/Yorri/.gemini