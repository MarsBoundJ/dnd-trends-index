from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT 
    t.date,
    l.category,
    m.concept_name,
    t.interest
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` AS t
JOIN `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` AS m ON CAST(t.term_id AS STRING) = CAST(m.term_id AS STRING)
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` AS l ON m.concept_name = l.concept_name
LIMIT 1
"""
try:
    results = client.query(query).result()
    for row in results:
        print(row)
except Exception as e:
    print(f"Error: {e}")
