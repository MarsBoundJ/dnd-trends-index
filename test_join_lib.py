from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT 
    m.original_keyword,
    l.concept_name,
    l.category
FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` m
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` l ON m.original_keyword = l.concept_name
WHERE l.concept_name = 'Fighter'
LIMIT 1
"""
try:
    results = client.query(query).result()
    for row in results:
        print(row)
except Exception as e:
    print(f"Error: {e}")
