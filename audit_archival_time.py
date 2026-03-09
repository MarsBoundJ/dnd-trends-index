from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
sql = """
SELECT query, creation_time
FROM `dnd-trends-index.region-us.INFORMATION_SCHEMA.JOBS`
WHERE query LIKE '%ai_suggestions%ARCHIVED%'
   OR query LIKE '%concept_library%is_active = FALSE%'
ORDER BY creation_time DESC
LIMIT 10
"""
print("--- Recent Archival Jobs ---")
for row in client.query(sql).result():
    print(f"Time: {row.creation_time} | Query: {row.query[:100]}...")
