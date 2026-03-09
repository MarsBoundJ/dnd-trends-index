from google.cloud import bigquery

client = bigquery.Client()
sql = "SELECT name FROM gold_data.view_social_leaderboards WHERE name = 'Me'"
results = list(client.query(sql, location='US').result())
print(f"Me Rows: {len(results)}")

sql2 = "SELECT canonical_concept FROM gold_data.view_api_leaderboards WHERE canonical_concept = 'Me'"
results2 = list(client.query(sql2, location='US').result())
print(f"Me API Rows: {len(results2)}")
