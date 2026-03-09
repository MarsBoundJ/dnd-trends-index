from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = "SELECT concept_name, category, tags FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE category = 'Subclass' LIMIT 10"
results = client.query(sql).result()
for row in results:
    print(f"[{row.category}] {row.concept_name} | Tags: {row.tags}")
