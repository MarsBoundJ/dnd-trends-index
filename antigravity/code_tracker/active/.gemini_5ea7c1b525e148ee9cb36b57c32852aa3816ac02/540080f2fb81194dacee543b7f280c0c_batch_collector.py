²7import os
import time
import random
import datetime
import uuid
import sys
import argparse
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import bigquery

# IP AUTH PROXY (Rotates automatically on 9999)
PROXY_URL = "http://p.webshare.io:9999"

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

BATCH_SIZE = 5
PAUSE_SECONDS = 10 

client = bigquery.Client(project=PROJECT_ID)

def fetch_unprocessed_terms(category=None):
    """
    Fetch only the terms we haven't successfully processed yet.
    Joins expanded_search_terms with trend_data_pilot to find gaps.
    Optionally filters by Category.
    """
    print(f"Fetching list of terms to process (Category: {category if category else 'ALL'})...")
    
    cat_filter = ""
    if category:
        # Handle comma-separated list
        cats = [f"'{c.strip()}'" for c in category.split(',')]
        cat_list = ",".join(cats)
        cat_filter = f"AND e.category IN ({cat_list})"

    query = f"""
        SELECT e.term_id, e.search_term 
        FROM `{SOURCE_TABLE}` e
        LEFT JOIN (
            SELECT DISTINCT search_term FROM `{DEST_TABLE}`
        ) t ON e.search_term = t.search_term
        WHERE t.search_term IS NULL
        {cat_filter}
    """
    return list(client.query(query).result())

def main():
    parser = argparse.ArgumentParser(description='Batch Trend Collector')
    parser.add_argument('--category', type=str, help='Category or comma-separated categories to process (e.g. Spell,Monster)', default=None)
    args = parser.parse_args()
    
    print("Starting Trend Batch Collector (IP Auth Rotating Proxy)...")
    print(f"Proxy: {PROXY_URL}")
    
    # 1. Fetch ONLY unprocessed terms for the target category
    terms = fetch_unprocessed_terms(args.category)
    print(f"Loaded {len(terms)} REMAINING terms to process.")
    
    if len(terms) == 0:
        print("All terms already processed! Exiting.")
        return
    
    batch_id = str(uuid.uuid4())
    all_term_details = {row['search_term']: row['term_id'] for row in terms}
    all_terms_list = list(all_term_details.keys())
    
    success_count = 0
    fail_count = 0
    
    # TRICK: Pytrends rotates through the list if it fails. 
    # Since our single URL rotates IPs on the server side, we just give it the same URL many times.
    proxies_list = [PROXY_URL] * 500
    
    total_batches = (len(all_terms_list) // BATCH_SIZE) + 1
    
    for i in range(0, len(all_terms_list), BATCH_SIZE):
        batch_terms = all_terms_list[i : i + BATCH_SIZE]
        current_batch = (i // BATCH_SIZE) + 1
        print(f"Batch {current_batch}/{total_batches}: {batch_terms}...")
        
        # Outer retry loop for the entire batch
        max_batch_retries = 3
        for attempt in range(max_batch_retries):
            try:
                # Re-init pytrends for every batch to ensure fresh state
                pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=1, proxies=proxies_list)
                
                pytrends.build_payload(batch_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
                data = pytrends.interest_over_time()
                
                if data.empty:
                    # Sometimes Google returns empty DF for niche terms. 
                    if attempt < max_batch_retries - 1:
                        print(f"  Empty response (attempt {attempt+1}). Retrying with new IP...")
                        time.sleep(5)
                        continue
                    else:
                        print("  No data returned after retries. Recording as 0 interest placeholder.")
                        rows_to_insert = []
                        fetched_at = datetime.datetime.now().isoformat()
                        # Use yesterday's date as a placeholder
                        placeholder_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
                        for term in batch_terms:
                            rows_to_insert.append({
                                "term_id": all_term_details.get(term),
                                "search_term": term,
                                "date": placeholder_date,
                                "interest": 0,
                                "is_partial": False,
                                "fetched_at": fetched_at,
                                "batch_id": batch_id
                            })
                        client.insert_rows_json(DEST_TABLE, rows_to_insert)
                        success_count += 1
                        break
                    
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
                
                break # Success on this batch, exit retry loop
                    
            except Exception as e:
                error_str = str(e)
                if "No more proxies" in error_str or "429" in error_str:
                    print(f"  Proxy exhausted or Rate Limit ({attempt+1}). Waiting 30s...")
                    time.sleep(30)
                else:
                    print(f"  Error (attempt {attempt+1}): {error_str[:100]}")
                    time.sleep(10)
                
                if attempt == max_batch_retries - 1:
                    fail_count += 1
        
        # Rate Limiting with jitter
        sleep_time = random.uniform(5, 12)
        time.sleep(sleep_time)

    print(f"\nBatch Collection Complete. Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    main()
Ñ *cascade08ÑÛ*cascade08Ûù *cascade08ùû*cascade08ûü *cascade08üý*cascade08ý	 *cascade08	†	*cascade08†	‡	 *cascade08‡	ˆ	*cascade08ˆ	ö *cascade08ö”*cascade08”²7 *cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac0221file:///C:/Users/Yorri/.gemini/batch_collector.py:file:///C:/Users/Yorri/.gemini