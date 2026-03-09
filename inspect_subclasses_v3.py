from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = "SELECT concept_name, category, tags FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE category LIKE '%Subclass%' LIMIT 50"
results = client.query(sql).result()
for row in results:
    tags = ", ".join(row.tags)
    print(f"Concept: {row.concept_name} | Tags: {tags}")
