from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
sql = 'SELECT concept_name, category, last_processed_at FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE is_active = FALSE ORDER BY last_processed_at DESC LIMIT 5'
for row in client.query(sql).result():
    print(f"{row.concept_name} ({row.category}) - {row.last_processed_at}")
