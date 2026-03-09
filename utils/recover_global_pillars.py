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

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    print("Surgical Recovery: Global Category Pillars (Top 50s + Subclasses)")
    
    # 1. Fetch concepts
    categories = ['Monster', 'Spell', 'MagicItem']
    concept_names = []
    
    table_path = f"`{PROJECT_ID}.{DATASET_ID}.concept_library`"
    
    for cat in categories:
        query = f"SELECT concept_name FROM {table_path} WHERE category = '{cat}' AND is_active = TRUE LIMIT 50"
        res = list(client.query(query).result())
        concept_names.extend([r['concept_name'] for r in res])
    
    subclass_query = f"SELECT concept_name FROM {table_path} WHERE category = 'Subclass' AND is_active = TRUE"
    res = list(client.query(subclass_query).result())
    concept_names.extend([r['concept_name'] for r in res])
    
    print(f"Targeting {len(concept_names)} concepts.")

    # 2. Map to expanded search terms using parameterization
    terms = []
    print("Mapping concepts to expanded search terms...")
    expanded_table = f"`{PROJECT_ID}.{DATASET_ID}.expanded_search_terms`"
    
    for chunk in chunk_list(concept_names, 100):
        query = f"SELECT term_id, search_term, original_keyword FROM {expanded_table} WHERE original_keyword IN UNNEST(@names)"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("names", "STRING", chunk)
            ]
        )
        terms.extend(list(client.query(query, job_config=job_config).result()))
    
    print(f"Found {len(terms)} search variations to fetch.")

    # 3. Collection Loop
    proxies_list = [PROXY_URL] * 10
    batch_id = str(uuid.uuid4())
    dest_table = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"
    
    CHUNK_SIZE = 5
    for i in range(0, len(terms), CHUNK_SIZE):
        batch = terms[i:i+CHUNK_SIZE]
        keywords = [b['search_term'] for b in batch]
        term_ids = {b['search_term']: b['term_id'] for b in batch}
        
        print(f"Batch {i//CHUNK_SIZE + 1}/{len(terms)//CHUNK_SIZE + 1}: {keywords}...")
        
        try:
            pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=2, proxies=proxies_list)
            pytrends.build_payload(keywords, cat=0, timeframe='today 12-m', geo='', gprop='')
            data = pytrends.interest_over_time()
            
            if not data.empty:
                rows_to_insert = []
                fetched_at = datetime.datetime.now().isoformat()
                for index, row in data.iterrows():
                    date_str = index.date().isoformat()
                    for kw in keywords:
                        if kw in data.columns:
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
