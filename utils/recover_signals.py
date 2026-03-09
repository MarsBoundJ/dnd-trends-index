import os
import uuid
import datetime
import time
import random
import sys
from pytrends.request import TrendReq
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def fetch_batch(batch_kws, term_map, dest_table):
    batch_id = str(uuid.uuid4())
    # Sanitize & to and for the API call payload
    api_kws = [kw.replace('&', 'and') for kw in batch_kws]
    api_to_orig = {kw.replace('&', 'and'): kw for kw in batch_kws}
    
    print(f"  [MANAGED] Fetching Batch: {batch_kws}", flush=True)
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=20, timeout=(30, 60))
        pytrends.build_payload(api_kws, cat=0, timeframe='today 12-m', geo='', gprop='')
        data = pytrends.interest_over_time()
        
        if not data.empty:
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                for api_kw in api_kws:
                    if api_kw in data.columns:
                        orig_kw = api_to_orig[api_kw]
                        rows_to_insert.append({
                            "term_id": term_map[orig_kw],
                            "search_term": orig_kw,
                            "date": date_str,
                            "interest": int(row[api_kw]),
                            "is_partial": bool(row.get('isPartial', False)) if 'isPartial' in data.columns else False,
                            "fetched_at": fetched_at,
                            "batch_id": batch_id
                        })
            
            if rows_to_insert:
                errors = client.insert_rows_json(dest_table, rows_to_insert)
                if not errors:
                    print(f"    [MANAGED] SUCCESS: {len(rows_to_insert)} rows.", flush=True)
                    return True
                else:
                    print(f"    [MANAGED] Insert Errors: {errors}", flush=True)
        else:
            print(f"    [MANAGED] No data returned for batch.", flush=True)
    except Exception as e:
        print(f"    [MANAGED] Batch Error: {e}", flush=True)
    return False

def main():
    print("ULTRA-ROBUST Signal Recovery Session (Unified Edition + Equipment)", flush=True)
    
    query = """
    SELECT t.term_id, t.search_term 
    FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` t
    LEFT JOIN `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` p ON t.term_id = p.term_id
    WHERE (t.category IN ('Edition', 'Equipment') OR t.is_pilot = TRUE)
    AND p.term_id IS NULL
    """
    
    terms = list(client.query(query).result())
    term_map = {t['search_term']: t['term_id'] for t in terms}
    search_list = list(term_map.keys())
    
    dest_table = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"
    
    random.shuffle(search_list)
    
    batch_size = 3
    for i in range(0, len(search_list), batch_size):
        batch = search_list[i:i+batch_size]
        print(f"Progress ({i+1}/{len(search_list)}): {batch}", flush=True)
        
        success = fetch_batch(batch, term_map, dest_table)
        
        if success:
            sleep_time = random.uniform(90, 150)
        else:
            sleep_time = random.uniform(300, 600) # Heavy backoff on failure
            print("  [WARNING] Batch failed. Entering extended cooldown...", flush=True)

        print(f"  [WAIT] Sleeping {sleep_time:.1f}s...", flush=True)
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
