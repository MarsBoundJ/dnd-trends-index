from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
query = 'SELECT DISTINCT category FROM `dnd-trends-index.dnd_trends_categorized.concept_library`'
results = client.query(query).result()
for row in results:
    print(row.category)
