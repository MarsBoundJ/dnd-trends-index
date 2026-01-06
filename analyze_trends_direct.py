from google.cloud import bigquery
import json

def characterize():
    client = bigquery.Client()
    
    # Corrected Locations based on 'bq ls'
    # trend_data_pilot is in dnd_trends_categorized
    # expanded_search_terms is in dnd_trends_categorized
    
    table_raw_id = "dnd-trends-index.dnd_trends_categorized.trend_data_pilot"
    table_meta_id = "dnd-trends-index.dnd_trends_categorized.expanded_search_terms"
    
    try:
        table_ref_raw = client.get_table(table_raw_id)
        table_ref_meta = client.get_table(table_meta_id)
        
        # Extract Count
        raw_rows = table_ref_raw.num_rows
        meta_rows = table_ref_meta.num_rows
        
        # Get schema
        raw_schema = [f.name for f in table_ref_raw.schema]
        meta_schema = [f.name for f in table_ref_meta.schema]
        
        # Get sample rows (Top 5)
        samples = [dict(row) for row in client.list_rows(table_ref_meta, max_results=5)]
        
        # Get date range for raw data via query
        query_dates = f"SELECT min(date) as start_date, max(date) as end_date FROM `{table_raw_id}`"
        date_job = client.query(query_dates, location=table_ref_raw.location)
        date_stats = list(date_job)[0]
        
        results = {
            "status": "Success",
            "trends": {
                "num_rows": raw_rows,
                "schema": raw_schema,
                "location": table_ref_raw.location,
                "dates": dict(date_stats)
            },
            "metadata": {
                "num_rows": meta_rows,
                "schema": meta_schema,
                "location": table_ref_meta.location
            },
            "samples": samples
        }
    except Exception as e:
        results = {
            "status": "Error",
            "message": str(e)
        }
    
    print(json.dumps(results, indent=2, default=str))

if __name__ == "__main__":
    characterize()
