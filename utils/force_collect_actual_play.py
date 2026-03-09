
import os
import time
import random
import datetime
import uuid
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import bigquery

# IP AUTH PROXY
PROXY_URL = "http://p.webshare.io:9999"

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def main():
    print("Surgical Recovery: Force Collect Actual Play Trends")
    
    # query to get terms for Actual Play that we want to RE-FETCH
    # We want to re-fetch anything in the 'Actual Play' category that we just expanded
    query = f"""
        SELECT e.term_id, e.search_term 
        FROM `{SOURCE_TABLE}` e
        JOIN `{PROJECT_ID}.{DATASET_ID}.concept_library` c ON e.original_keyword = c.concept_name
        WHERE c.category = 'Actual Play' AND c.is_active = TRUE
    """
    rows = list(client.query(query).result())
    print(f"Total terms to re-fetch: {len(rows)}")

    # We will process in small batches of 2 to avoid the 'scaling to zero' issue 
    # and keep it high fidelity.
    BATCH_SIZE = 2 
    proxies_list = [PROXY_URL] * 100
    batch_id = str(uuid.uuid4())

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        batch_terms = [r['search_term'] for r in batch]
        term_map = {r['search_term']: r['term_id'] for r in batch}
        
        print(f"Batch {i//BATCH_SIZE + 1}: {batch_terms}...")
        
        try:
            pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=1, proxies=proxies_list)
            pytrends.build_payload(batch_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
            data = pytrends.interest_over_time()
            
            if not data.empty:
                rows_to_insert = []
                fetched_at = datetime.datetime.now().isoformat()
                
                for index, row in data.iterrows():
                    date_str = index.date().isoformat()
                    for term in batch_terms:
                        if term in data.columns:
                            rows_to_insert.append({
                                "term_id": term_map.get(term),
                                "search_term": term,
                                "date": date_str,
                                "interest": int(row[term]),
                                "is_partial": bool(row.get('isPartial', False)) if 'isPartial' in data.columns else False,
                                "fetched_at": fetched_at,
                                "batch_id": batch_id
                            })
                
                if rows_to_insert:
                    # We use a temp table or just insert and rely on the View logic (MAX scores)
                    # Actually, the view picks MAX(avg_interest), so having more data is fine 
                    # as long as the new data is higher quality.
                    # But cleaning up the zeros would be better. 
                    # Since we can't DELETE from buffer, we'll just insert and the MAX will win.
                    errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                    if errors:
                        print(f"  BQ Error: {errors}")
                    else:
                        print(f"  SUCCESS: {len(rows_to_insert)} rows.")
            else:
                print("  No data returned.")
                
        except Exception as e:
            print(f"  Error: {e}")
            
        time.sleep(random.uniform(5, 10))

if __name__ == "__main__":
    main()
