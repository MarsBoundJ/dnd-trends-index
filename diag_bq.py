from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

query = """
    SELECT 
        keyword as name, 
        category,
        trend_score_raw as score
    FROM `dnd-trends-index.gold_data.trend_scores`
    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.gold_data.trend_scores`)
    LIMIT 5
"""

print("Running diagnostic query...")
try:
    results = client.query(query).to_dataframe()
    print("Results found:")
    print(results)
except Exception as e:
    print(f"Error: {e}")
