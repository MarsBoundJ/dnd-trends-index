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
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.concept_library"
EXPANDED_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def main():
    print("Surgical Recovery: Cartographer Category")
    
    # 1. Expand search terms
    print("Fetching Cartographer concepts...")
    query = f"SELECT concept_name, category FROM `{SOURCE_TABLE}` WHERE category = 'Cartographer' AND is_active = TRUE"
    concepts = list(client.query(query).result())
    print(f"Found {len(concepts)} active Cartographer concepts.")

    print("Checking for existing expansions...")
    existing_query = f"SELECT original_keyword, search_term FROM `{EXPANDED_TABLE}` WHERE category = 'Cartographer'"
    existing_rows = list(client.query(existing_query).result())
    existing_map = {(row['original_keyword'], row['search_term']) for row in existing_rows}

    new_expansions = []
    created_at = datetime.datetime.now().isoformat()
    
    for concept in concepts:
        name = concept['concept_name']
        cat = concept['category']
        
        # Rule: Standalone, Dnd, 5e
        targets = [name, f"{name} Dnd", f"{name} 5e"]
        for t in targets:
            if (name, t) not in existing_map:
                new_expansions.append({
                    "term_id": str(uuid.uuid4()),
                    "original_keyword": name,
                    "category": cat,
                    "search_term": t,
                    "expansion_rule": "cartographer_recovery",
                    "created_at": created_at,
                    "is_pilot": True
                })

    if new_expansions:
        print(f"Inserting {len(new_expansions)} new search terms...")
        client.insert_rows_json(EXPANDED_TABLE, new_expansions)
    else:
        print("No new expansions needed.")

    # 2. Fetch Trends
    print("Fetching Trends for Cartographer category...")
    fetch_query = f"""
        SELECT e.term_id, e.search_term 
        FROM `{EXPANDED_TABLE}` e
        JOIN `{SOURCE_TABLE}` c ON e.original_keyword = c.concept_name
        WHERE c.category = 'Cartographer' AND c.is_active = TRUE
    """
    rows_to_fetch = list(client.query(fetch_query).result())
    print(f"Total terms to fetch: {len(rows_to_fetch)}")

    BATCH_SIZE = 2 
    proxies_list = [PROXY_URL] * 100
    batch_id = str(uuid.uuid4())

    for i in range(0, len(rows_to_fetch), BATCH_SIZE):
        batch = rows_to_fetch[i : i + BATCH_SIZE]
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
                    errors = client.insert_rows_json(DEST_TABLE, rows_to_insert)
                    if not errors:
                        print(f"  SUCCESS: {len(rows_to_insert)} rows.")
                    else:
                        print(f"  BQ Error: {errors}")
            else:
                print("  No data returned.")
                
        except Exception as e:
            print(f"  Error: {e}")
            
        time.sleep(random.uniform(5, 10))

if __name__ == "__main__":
    main()
