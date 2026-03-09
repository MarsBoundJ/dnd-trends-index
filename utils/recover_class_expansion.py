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
    print(f"  Fetching: {batch_kws}", flush=True)
    try:
        # Direct fetching with safe headers/params
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=10)
            
        pytrends.build_payload(batch_kws, cat=0, timeframe='today 12-m', geo='', gprop='')
        data = pytrends.interest_over_time()
        
        if not data.empty:
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                for kw in batch_kws:
                    if kw in data.columns:
                        rows_to_insert.append({
                            "term_id": term_map[kw],
                            "search_term": kw,
                            "date": date_str,
                            "interest": int(row[kw]),
                            "is_partial": bool(row.get('isPartial', False)) if 'isPartial' in data.columns else False,
                            "fetched_at": fetched_at,
                            "batch_id": batch_id
                        })
            
            if rows_to_insert:
                errors = client.insert_rows_json(dest_table, rows_to_insert)
                if not errors:
                    print(f"    SUCCESS: {len(rows_to_insert)} rows.", flush=True)
                    return True
                else:
                    print(f"    Insert Errors: {errors}", flush=True)
        else:
            print("    No data returned.", flush=True)
    except Exception as e:
        print(f"    Batch Error: {e}", flush=True)
    return False

def main():
    print("Surgical Recovery: Class Calibration & Signal Expansion (Direct Mode)", flush=True)
    
    query = f"""
    SELECT DISTINCT term_id, search_term 
    FROM `{PROJECT_ID}.{DATASET_ID}.expanded_search_terms`
    WHERE is_pilot = TRUE
    AND (
        search_term LIKE '% 2024' OR 
        search_term LIKE '% dnd' OR 
        search_term LIKE '% Dnd' OR 
        search_term LIKE '% dnd class' OR 
        search_term LIKE '% dnd build' OR
        search_term LIKE '% class 5e' OR
        search_term LIKE '% mcdm' OR
        search_term LIKE '% Nat19' OR
        search_term LIKE '% dungeon dudes' OR
        original_keyword IN ('Blood Hunter', 'Bender', 'Inscriptor', 'Sword Saint', 'Apothecary')
    )
    """
    
    try:
        terms = list(client.query(query).result())
        term_map = {t['search_term']: t['term_id'] for t in terms}
        search_list = list(term_map.keys())
        
        # Filter out terms that already have data in trend_data_pilot for the last 12 months
        # To avoid redundant fetching
        print(f"Total potential terms: {len(search_list)}", flush=True)
        
        batch_size = 5
        dest_table = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"
        
        random.shuffle(search_list) # Jitter the order
        
        for i in range(0, len(search_list), batch_size):
            batch_kws = search_list[i:i+batch_size]
            print(f"Batch {i//batch_size + 1}/{len(search_list)//batch_size + 1}", flush=True)
            
            success = fetch_batch(batch_kws, term_map, dest_table)
            
            if not success:
                print("    RECOVERY FAILED for this batch. Sleeping longer...", flush=True)
                time.sleep(60)
            
            # Substantial sleep to avoid rate limiting
            time.sleep(random.uniform(20, 40))
                
    except Exception as e:
        print(f"Fatal Error: {e}", flush=True)

if __name__ == "__main__":
    main()
