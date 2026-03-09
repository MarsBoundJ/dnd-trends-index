from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT m.*, l.category 
FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` m 
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` l ON m.concept_name = l.concept_name 
LIMIT 1
"""
try:
    results = list(client.query(query).result())
    if results:
        print(f"Row Keys: {results[0].keys()}")
    else:
        print("No results found.")
except Exception as e:
    print(f"Error: {e}")
