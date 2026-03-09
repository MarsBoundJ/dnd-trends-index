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

def fetch_safe(kw, term_id, dest_table):
    batch_id = str(uuid.uuid4())
    # Standardize & to and for the API call
    api_kw = kw.replace('&', 'and')
    print(f"  [MANAGED] Fetching: {kw} (as {api_kw})", flush=True)
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=20, timeout=(20, 40))
        pytrends.build_payload([api_kw], cat=0, timeframe='today 12-m', geo='', gprop='')
        data = pytrends.interest_over_time()
        
        if not data.empty:
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                if api_kw in data.columns:
                    rows_to_insert.append({
                        "term_id": term_id,
                        "search_term": kw, # Keep original kw for our logs
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
            print(f"    [MANAGED] No data returned for {api_kw}.", flush=True)
    except Exception as e:
        print(f"    [MANAGED] Error for {kw}: {e}", flush=True)
    return False

def main():
    print("ULTRA-ROBUST Edition Recovery Session (v2)", flush=True)
    
    query = f"SELECT term_id, search_term FROM `{PROJECT_ID}.{DATASET_ID}.expanded_search_terms` WHERE category = 'Edition'"
    terms = list(client.query(query).result())
    dest_table = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"
    
    # Prioritize 5e and 2024
    prioritized = [t for t in terms if '5e' in t['search_term'] or '2024' in t['search_term']]
    others = [t for t in terms if t not in prioritized]
    random.shuffle(prioritized)
    random.shuffle(others)
    terms = prioritized + others
    
    for i, t in enumerate(terms):
        kw = t['search_term']
        term_id = t['term_id']
        
        # Check if already has data
        check_query = f"SELECT count(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.trend_data_pilot` WHERE term_id = '{term_id}'"
        res = list(client.query(check_query).result())
        if res and res[0]['count'] > 50:
            print(f"[{i+1}/{len(terms)}] {kw} - SKIPPING", flush=True)
            continue

        print(f"[{i+1}/{len(terms)}] {kw} - PROCESSING", flush=True)
        success = fetch_safe(kw, term_id, dest_table)
        
        if success:
            sleep_time = random.uniform(40, 60)
        else:
            sleep_time = random.uniform(120, 180)
            print("  [WARNING] Failed. Cooling down...", flush=True)

        print(f"  [WAIT] Sleeping {sleep_time:.1f}s...", flush=True)
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
