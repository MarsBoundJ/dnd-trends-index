import os
import uuid
import datetime
import time
from pytrends.request import TrendReq
from google.cloud import bigquery

# IP AUTH PROXY
PROXY_URL = "http://p.webshare.io:9999"
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
EXPANDED_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def main():
    print("Surgical Collection: Refined Actual Play Variants")
    
    terms_to_collect = ["Avantris Surpasa", "Titansgrave", "Legends of Avantris Surpasa"]
    placeholders = ",".join([f"'{t}'" for t in terms_to_collect])
    
    query = f"""
        SELECT term_id, search_term 
        FROM `{EXPANDED_TABLE}` 
        WHERE search_term IN ({placeholders})
    """
    rows = list(client.query(query).result())
    
    if not rows:
        print("No matching terms found in database.")
        return

    term_map = {r['search_term']: r['term_id'] for r in rows}
    search_terms = list(term_map.keys())
    
    proxies_list = [PROXY_URL] * 10
    batch_id = str(uuid.uuid4())
    
    print(f"Collecting Trends for: {search_terms}...")
    
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=1, proxies=proxies_list)
        pytrends.build_payload(search_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
        data = pytrends.interest_over_time()
        
        if not data.empty:
            rows_to_insert = []
            fetched_at = datetime.datetime.now().isoformat()
            for index, row in data.iterrows():
                date_str = index.date().isoformat()
                for term in search_terms:
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
                errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                if not errors:
                    print(f"Successfully inserted {len(rows_to_insert)} rows of trend data.")
                else:
                    print(f"BQ Insertion Errors: {errors}")
        else:
            print("No data returned from Google Trends.")
            
    except Exception as e:
        print(f"Error during collection: {e}")

if __name__ == "__main__":
    main()
