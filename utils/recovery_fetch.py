
import os
import time
import random
import datetime
import uuid
from pytrends.request import TrendReq
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

BATCH_SIZE = 5 
PAUSE_SECONDS = 15

# Explicitly use the key file if it exists
key_path = "/workspaces/dnd-trends/dnd-key.json"
if os.path.exists(key_path):
    client = bigquery.Client.from_service_account_json(key_path, project=PROJECT_ID)
else:
    client = bigquery.Client(project=PROJECT_ID)

def fetch_missing_terms():
    query = f"""
        SELECT e.term_id, e.search_term 
        FROM `{SOURCE_TABLE}` e
        LEFT JOIN `{DEST_TABLE}` t ON e.search_term = t.search_term
        WHERE e.is_pilot = TRUE AND t.search_term IS NULL
    """
    return list(client.query(query).result())

def main():
    print("Starting Recovery Fetch...")
    terms = fetch_missing_terms()
    print(f"Loaded {len(terms)} missing terms.")
    if not terms:
        return

    # Increase timeout for the session
    proxies_list = [PROXY_URL] if PROXY_URL else []
    
    print(f"Initializing TrendReq with proxy: {bool(proxies_list)}...")
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=1.0, proxies=proxies_list, timeout=(10, 25))
    except Exception as e:
        print(f"Failed to init with proxy: {e}. Trying without proxy...")
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=1.0, timeout=(10, 25))

    batch_id = f"recovery-{str(uuid.uuid4())[:8]}"

    all_term_details = {row['search_term']: row['term_id'] for row in terms}
    all_terms_list = list(all_term_details.keys())

    for i in range(0, len(all_terms_list), BATCH_SIZE):
        batch_terms = all_terms_list[i : i + BATCH_SIZE]
        print(f"Batch {i//BATCH_SIZE + 1}: {batch_terms}")
        
        try:
            pytrends.build_payload(batch_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
            data = pytrends.interest_over_time()
            if data.empty:
                print("No data.")
                continue
                
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                is_partial = bool(row['isPartial']) if 'isPartial' in data.columns else False

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
                if errors: print(f"BQ Error: {errors}")
                else: print(f"Saved {len(rows_to_insert)} rows.")
            
        except Exception as e:
            print(f"Failed: {e}")
            if "429" in str(e): time.sleep(60)
        
        time.sleep(PAUSE_SECONDS + random.uniform(1, 5))

    print("Recovery Fetch Complete.")

if __name__ == "__main__":
    main()
