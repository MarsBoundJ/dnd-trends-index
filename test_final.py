from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT 
    t.date,
    l.category,
    l.concept_name,
    t.interest
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
JOIN `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` m ON t.term_id = m.term_id
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` l ON m.concept_name = l.concept_name
LIMIT 1
"""
try:
    results = client.query(query).result()
    for row in results:
        print(row)
except Exception as e:
    print(f"Error: {e}")
