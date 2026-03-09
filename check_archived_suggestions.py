from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
sql = "SELECT concept_name, status FROM `dnd-trends-index.dnd_trends_raw.ai_suggestions` WHERE status = 'ARCHIVED'"
print("--- ARCHIVED Suggestions ---")
for row in client.query(sql).result():
    print(f"Concept: {row.concept_name}")
