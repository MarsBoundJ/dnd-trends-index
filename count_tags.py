from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = """
SELECT 
    COUNTIF(REGEXP_CONTAINS(tag, r'_Subclasses$')) as matching,
    COUNT(*) as total
FROM `dnd-trends-index.dnd_trends_categorized.concept_library` lib,
UNNEST(tags) as tag
WHERE LOWER(category) = 'subclass'
"""
results = client.query(sql).result()
for row in results:
    print(dict(row))
