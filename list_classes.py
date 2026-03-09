from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = "SELECT concept_name FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE category = 'Class' LIMIT 20"
results = client.query(sql).result()
for row in results:
    print(row.concept_name)
