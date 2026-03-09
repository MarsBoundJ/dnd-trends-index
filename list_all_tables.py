from google.cloud import bigquery

client = bigquery.Client()

print("\n--- view_google_mapping schema ---")
try:
    query = "SELECT * FROM `dnd-trends-index.silver_data.view_google_mapping` LIMIT 0"
    res = client.query(query).result()
    print([f.name for f in res.schema])
except Exception as e:
    print(f"Error: {e}")
