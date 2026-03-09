from google.cloud import bigquery
import os

client = bigquery.Client('dnd-trends-index')

# Read the validated query
with open('/app/api_query.sql', 'r') as f:
    view_sql = f.read()

full_query = f"CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_api_leaderboards` AS {view_sql}"

print("Refreshing view_api_leaderboards...")
try:
    client.query(full_query).result()
    print("VIEW REFRESHED successfully.")
except Exception as e:
    print(f"Error: {e}")
