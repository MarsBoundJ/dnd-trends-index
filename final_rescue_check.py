from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
sql = "SELECT concept_name FROM `dnd-trends-index.dnd_trends_raw.ai_suggestions` WHERE status = 'ARCHIVED'"
print("--- Recently Archived from Suggestions ---")
for row in client.query(sql).result():
    print(f"- {row.concept_name}")

sql2 = "SELECT concept_name, category FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE is_active = FALSE ORDER BY last_processed_at DESC LIMIT 5"
print("\n--- Top 5 Library Archives (is_active = FALSE) ---")
for row in client.query(sql2).result():
    print(f"- {row.concept_name} ({row.category})")
