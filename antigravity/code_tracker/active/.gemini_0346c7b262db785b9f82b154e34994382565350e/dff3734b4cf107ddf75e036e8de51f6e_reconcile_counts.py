¶
from google.cloud import bigquery
import json

def ReconcileKeywords():
    client = bigquery.Client()
    results = {}

    queries = {
        "concept_library_rows": "SELECT count(*) FROM `dnd-trends-index.dnd_trends_categorized.concept_library` ",
        "concept_library_unique": "SELECT count(DISTINCT concept_name) FROM `dnd-trends-index.dnd_trends_categorized.concept_library` ",
        "expanded_unique_orig": "SELECT count(DISTINCT original_keyword) FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` ",
        "expanded_unique_search": "SELECT count(DISTINCT search_term) FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` ",
        "pilot_unique_keyword": "SELECT count(DISTINCT keyword) FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` ",
        "raw_discovered_rows": "SELECT count(*) FROM `dnd-trends-index.dnd_trends_raw.raw_discovered_queries` "
    }

    for key, sql in queries.items():
        print(f"Running: {key}...")
        try:
            query_job = client.query(sql)
            res = list(query_job.result())
            results[key] = res[0][0]
        except Exception as e:
            results[key] = f"Error: {str(e)}"

    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    ReconcileKeywords()
¶
"(0346c7b262db785b9f82b154e34994382565350e22file:///C:/Users/Yorri/.gemini/reconcile_counts.py:file:///C:/Users/Yorri/.gemini