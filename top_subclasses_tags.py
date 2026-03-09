from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = """
SELECT lib.concept_name, lib.tags, SUM(t.interest) as total_interest
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
JOIN `dnd-trends-index.silver_data.view_google_mapping` m ON t.term_id = m.term_id
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` lib ON m.canonical_concept = lib.concept_name
WHERE LOWER(lib.category) = 'subclass'
GROUP BY 1, 2
ORDER BY total_interest DESC
LIMIT 20
"""
results = client.query(sql).result()
for row in results:
    print(f"{row.concept_name} (Interest: {row.total_interest}) | Tags: {row.tags}")
