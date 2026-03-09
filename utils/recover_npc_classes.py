import os
import uuid
import datetime
import time
import random
from pytrends.request import TrendReq
from google.cloud import bigquery

# IP AUTH PROXY
PROXY_URL = "http://p.webshare.io:9999"
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def main():
    print("Surgical Recovery: Qualified NPC Classes")
    
    keywords = [
        "Captain class 5e", "Captain class dnd",
        "Merchant class 5e", "Merchant class dnd",
        "Scholar class 5e", "Scholar class dnd"
    ]
    
    # 1. Get term IDs
    expanded_table = f"`{PROJECT_ID}.{DATASET_ID}.expanded_search_terms`"
    placeholders = ",".join(["'" + k.replace("'", "''") + "'" for k in keywords])
    query = f"SELECT term_id, search_term FROM {expanded_table} WHERE search_term IN ({placeholders})"
    terms = list(client.query(query).result())
    term_ids = {t['search_term']: t['term_id'] for t in terms}
    
    # 2. Fetch from Google Trends
    proxies_list = [PROXY_URL]
    batch_id = str(uuid.uuid4())
    dest_table = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"
    
    # Split keywords into batches of 5
    batch_size = 5
    for i in range(0, len(keywords), batch_size):
        chunk = keywords[i:i+batch_size]
        print(f"Fetching batch: {chunk}")
        
        try:
            pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=2, proxies=proxies_list)
            pytrends.build_payload(chunk, cat=0, timeframe='today 12-m', geo='', gprop='')
            data = pytrends.interest_over_time()
            
            if not data.empty:
                rows_to_insert = []
                fetched_at = datetime.datetime.now().isoformat()
                for index, row in data.iterrows():
                    date_str = index.date().isoformat()
                    for kw in chunk:
                        if kw in data.columns and kw in term_ids:
                            rows_to_insert.append({
                                "term_id": term_ids[kw],
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
                        print(f"  SUCCESS: {len(rows_to_insert)} rows.")
                    else:
                        print(f"  Insert Errors: {errors}")
            else:
                print("  No data returned.")
            
            time.sleep(random.uniform(5, 10))
            
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
