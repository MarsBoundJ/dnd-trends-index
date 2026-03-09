from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

print("=== Persona Audit ===")
query = "SELECT persona_target, COUNT(*) as count FROM `dnd-trends-index.dnd_trends_categorized.concept_library` GROUP BY 1 ORDER BY 2 DESC"
for row in client.query(query).result():
    print(f"{row.persona_target}: {row.count}")

print("\n=== Milestone Audit ===")
query = "SELECT event_name, event_date, event_type FROM `dnd-trends-index.gold_data.event_milestones` ORDER BY event_date ASC"
for row in client.query(query).result():
    print(f"{row.event_name} | {row.event_date} | {row.event_type}")

print("\n=== BGG Health Check (Latest Data) ===")
query = "SELECT MAX(extraction_date) FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`"
res = list(client.query(query).result())
print(f"Latest BGG Extraction: {res[0][0]}")
