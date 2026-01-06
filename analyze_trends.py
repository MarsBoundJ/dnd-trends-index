from google.cloud import bigquery
import json

def characterize():
    client = bigquery.Client()
    
    # 1. Stats for trend_data_pilot
    query_raw = """
    SELECT 
        count(*) as total_rows, 
        count(distinct query) as unique_keywords, 
        min(date) as start_date, 
        max(date) as end_date 
    FROM `dnd-trends-index.dnd_trends_raw.trend_data_pilot`
    """
    raw_stats = list(client.query(query_raw, location='US'))[0]
    
    # 2. Stats for expanded_search_terms
    query_meta = """
    SELECT 
        count(*) as total_rows, 
        count(distinct keyword) as unique_keywords
    FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
    """
    meta_stats = list(client.query(query_meta, location='US'))[0]
    
    # 3. Sample metadata
    query_sample = """
    SELECT keyword, category, source_book, is_official 
    FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
    LIMIT 5
    """
    samples = [dict(row) for row in client.query(query_sample, location='US')]
    
    results = {
        "raw_stats": dict(raw_stats),
        "meta_stats": dict(meta_stats),
        "samples": samples
    }
    
    print(json.dumps(results, indent=2, default=str))

if __name__ == "__main__":
    characterize()
