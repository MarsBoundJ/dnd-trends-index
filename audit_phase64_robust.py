from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

# 1. Persona Audit
print("--- Persona Audit ---")
query = "SELECT persona_target, COUNT(*) as count FROM `dnd-trends-index.dnd_trends_categorized.concept_library` GROUP BY 1 ORDER BY 2 DESC"
for row in client.query(query).result():
    print(f"{row[0]}: {row[1]}")

# 2. Milestone Audit
print("\n--- Milestone Audit ---")
query = "SELECT event_name, event_date, event_type FROM `dnd-trends-index.gold_data.event_milestones` ORDER BY event_date ASC"
for row in client.query(query).result():
    print(f"{row.event_name} | {row.event_date} | {row.event_type}")

# 3. BGG Status
print("\n--- BGG Status ---")
query = "SELECT MAX(extraction_date) as max_date, COUNT(*) as total_rows FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`"
res = list(client.query(query).result())[0]
print(f"Latest BGG Extraction: {res.max_date}")
print(f"Total BGG Rows: {res.total_rows}")
