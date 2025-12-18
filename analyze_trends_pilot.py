
import os
import time
import random
import datetime
import uuid
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import bigquery
from dotenv import load_dotenv

# Load Proxy from .env
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

# Control term to normalize data (optional for now, but good practice)
# We will just run batches of 5 for now to maximize throughput, 
# as normalization logic is a complex Phase 3 step.
# For this pilot, we just want raw interest to verify we can GET data.
BATCH_SIZE = 5 
PAUSE_SECONDS = 15 # Be polite to Google

client = bigquery.Client(project=PROJECT_ID)

def get_proxy_dict():
    if not PROXY_URL:
        return None
    return {
        "http": PROXY_URL,
        "https": PROXY_URL
    }

def fetch_terms():
    """Fetches terms that haven't been processed yet? 
    For Pilot, we just fetch all terms marked is_pilot=True.
    In a real system, we'd check a 'processed' flag.
    For now, let's just fetch all 1600."""
    query = f"""
        SELECT term_id, search_term 
        FROM `{SOURCE_TABLE}`
        WHERE is_pilot = TRUE
    """
    return list(client.query(query).result())

def save_to_bq(df, batch_id):
    """Inserts dataframe to BigQuery"""
    if df.empty:
        return
    
    rows_to_insert = []
    fetched_at = datetime.datetime.now().isoformat()
    
    for index, row in df.iterrows():
        # df index is date
        date_obj = index.date()
        
        for col in df.columns:
            if col == 'isPartial':
                continue
            
            # col is the search_term. We need to map back to term_id?
            # Ideally we passed a map.
             # For now, let's just insert search_term and handle ID matching in join
            interest = row[col]
            
            rows_to_insert.append({
                "term_id": None, # Pytrends doesn't return our ID. We need to map it.
                "search_term": col,
                "date": date_obj.isoformat(),
                "interest": int(interest),
                "is_partial": row.get('isPartial', False) if 'isPartial' in row else False,
                "fetched_at": fetched_at,
                "batch_id": batch_id
            })
            
    # Insert
    errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
    if errors:
        print(f"BQ Errors: {errors}")
    else:
        print(f"Saved {len(rows_to_insert)} rows to BQ.")

def match_terms_to_ids(terms_batch, df_columns):
    # Helper to map back? 
    # Actually, we know the batch we sent.
    pass

def main():
    print("Starting Trend Analysis Pilot...")
    
    # 1. Fetch Terms
    terms = fetch_terms()
    print(f"Loaded {len(terms)} terms.")
    
    # 2. Setup Pytrends
    # Note: proxies arg in TrendReq is for the requests lib
    # format: ["http://user:pass@host:port"]
    proxies_list = [PROXY_URL] if PROXY_URL else []
    
    # Init pytrends
    # retries=2, backoff_factor=0.1
    pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=1.0, proxies=proxies_list)
    
    batch_id = str(uuid.uuid4())
    
    # 3. Batch Process
    # We map {search_term: term_id} for the current batch
    
    all_term_details = {row['search_term']: row['term_id'] for row in terms}
    all_terms_list = list(all_term_details.keys())
    
    for i in range(0, len(all_terms_list), BATCH_SIZE):
        batch_terms = all_terms_list[i : i + BATCH_SIZE]
        print(f"Processing Batch {i//BATCH_SIZE + 1}: {batch_terms}")
        
        try:
            # Build Payload
            pytrends.build_payload(batch_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
            
            # Get Data
            data = pytrends.interest_over_time()
            
            if data.empty:
                print("No data returned for batch.")
                continue
                
            # Transform and Save
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                is_partial = False
                if 'isPartial' in data.columns:
                     # isPartial column exists, but usually refers to the row? 
                     # Pytrends returns isPartial as a column sometimes.
                     is_partial = bool(row['isPartial']) if 'isPartial' in row else False

                for term in batch_terms:
                    if term in data.columns:
                        interest = row[term]
                        term_id = all_term_details.get(term)
                        
                        rows_to_insert.append({
                            "term_id": term_id,
                            "search_term": term,
                            "date": date_str,
                            "interest": int(interest),
                            "is_partial": is_partial,
                            "fetched_at": fetched_at,
                            "batch_id": batch_id
                        })
            
            # Upload
            if rows_to_insert:
                errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                if errors:
                    print(f"Error inserting batch: {errors}")
                else:
                    print(f"Success. {len(rows_to_insert)} rows inserted.")
            
        except Exception as e:
            print(f"Failed batch {i}: {e}")
            # Identify 429s specifically?
            if "429" in str(e):
                print("Rate limited! Sleeping extra long...")
                time.sleep(60)
        
        if i > 0 and i % 50 == 0:
            print(f"Progress checkpoint: {i} processed...")
        
        # Rate Limiting
        sleep_time = PAUSE_SECONDS + random.uniform(1, 5)
        print(f"Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)

    print("Pilot Run Complete.")

if __name__ == "__main__":
    main()
