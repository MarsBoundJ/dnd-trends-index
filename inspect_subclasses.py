from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
query = "SELECT concept_name, category, canonical_parent FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE category = 'Subclass' LIMIT 10"
results = client.query(query).result()
for row in results:
    print(dict(row))
