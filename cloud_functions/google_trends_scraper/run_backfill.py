import traceback
import datetime
import uuid
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import bigquery
import time
import random

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

client = bigquery.Client(project=PROJECT_ID)

limit = 10
start_date = '2026-02-01'
end_date = '2026-03-01'

# High quality User-Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (AppleChromium) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

query = f"""
    SELECT term_id, search_term
    FROM `{SOURCE_TABLE}`
    WHERE is_pilot = TRUE
    LIMIT {limit}
"""
terms = list(client.query(query).result())

batch_id = str(uuid.uuid4())
print(f"🚀 Starting STEALTH backfill for {len(terms)} terms: {start_date} to {end_date}.")

proxy_url = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

success_count = 0

for term_row in terms:
    term = term_row.search_term
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Processing: {term}")
    
    try:
        ua = random.choice(USER_AGENTS)
        # Custom headers via requests_args
        requests_args = {
            'headers': {
                'User-Agent': ua,
            }
        }
        
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=3, timeout=(25, 60), 
                           proxies=[proxy_url], requests_args=requests_args)
        
        pytrends.build_payload([term], cat=0, timeframe=f'{start_date} {end_date}', geo='', gprop='')
        data = pytrends.interest_over_time()
        
        if data is None or data.empty:
            print(f"  [LOG] No data for {term}")
        else:
            print(f"  [LOG] Found {len(data)} points.")
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            
            for date_idx, row in data.iterrows():
                rows_to_insert.append({
                    "term_id": term_row.term_id,
                    "search_term": term,
                    "date": date_idx.date().isoformat(),
                    "interest": int(row[term]),
                    "is_partial": bool(row.get('isPartial', False)),
                    "fetched_at": fetched_at,
                    "batch_id": "BACKFILL_" + batch_id
                })
                
            if rows_to_insert:
                errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                if errors:
                    print(f"  BQ Error: {errors}")
                else:
                    print(f"  Inserted {len(rows_to_insert)} rows.")
                    success_count += 1
        
        # VERY generous sleep for stealth
        sleep_time = random.uniform(25, 45)
        print(f"  Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)
        
    except Exception as e:
        err_str = str(e)
        print(f"  [ERROR] {term}: {err_str}")
        if '429' in err_str or 'Too Many Requests' in err_str:
            print("🚨 429 Detected. Cooling down for 120s...")
            time.sleep(120)
        elif '407' in err_str:
             print("🚨 407 Detected. Retrying with next IP...")
             time.sleep(10)

print(f"✅ STEALTH backfill process completed. Successes: {success_count}/{len(terms)}")
