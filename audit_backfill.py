from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

query = """
    SELECT search_term, COUNT(*) as points, MIN(date) as min_date, MAX(date) as max_date 
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` 
    WHERE batch_id LIKE 'BACKFILL_%' 
    GROUP BY search_term
"""
print("Backfill Results Audit:")
results = client.query(query).result()
for row in results:
    print(f"{row.search_term}: {row.points} points ({row.min_date} to {row.max_date})")
