from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
query = """
SELECT 
    concept_name, 
    category, 
    sentiment_label, 
    sentiment_score, 
    verdict, 
    context_quote, 
    reported_not_creator 
FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence` 
LIMIT 5
"""
try:
    df = client.query(query).to_dataframe()
    if df.empty:
        print("No data found in yt_video_intelligence.")
    else:
        print(df.to_markdown(index=False))
except Exception as e:
    print(f"Report Generation Failed: {e}")
