from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

print("--- Persona Audit ---")
try:
    query = "SELECT persona_target, COUNT(*) as count FROM `dnd-trends-index.dnd_trends_categorized.concept_library` GROUP BY 1 ORDER BY 2 DESC"
    for row in client.query(query).result():
        print(f"{row.persona_target}: {row.count}")
except Exception as e:
    print(f"Persona Audit Error: {e}")

print("\n--- Milestone Audit ---")
try:
    query = "SELECT event_name, event_date, event_type FROM `dnd-trends-index.gold_data.event_milestones` ORDER BY event_date ASC"
    for row in client.query(query).result():
        print(f"{row.event_name} | {row.event_date} | {row.event_type}")
except Exception as e:
    print(f"Milestone Audit Error: {e}")

print("\n--- BGG Data Integrity Check ---")
try:
    # Check max date in stats table
    query = "SELECT MAX(extraction_date) as max_date, COUNT(*) as total_rows FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`"
    res = list(client.query(query).result())[0]
    print(f"Latest BGG Extraction: {res.max_date}")
    print(f"Total BGG Rows: {res.total_rows}")
    
    # Check if there was any data for March 3rd
    query = "SELECT COUNT(*) FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats` WHERE DATE(extraction_date) = '2026-03-03'"
    count_today = list(client.query(query).result())[0][0]
    print(f"Rows from Today (March 3rd): {count_today}")
except Exception as e:
    print(f"BGG Integrity Error: {e}")
