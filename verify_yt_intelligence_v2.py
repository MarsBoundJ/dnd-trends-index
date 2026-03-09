from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
table_id = "dnd-trends-index.dnd_trends_raw.yt_video_intelligence"
table = client.get_table(table_id)

# list_rows is more reliable for streaming buffer than query immediately after insert
rows = client.list_rows(table, max_results=5)
data = [dict(row) for row in rows]

if not data:
    print("No data found in yt_video_intelligence.")
else:
    df = pd.DataFrame(data)
    # Reorder columns for readability if they exist
    cols = ['concept_name', 'category', 'sentiment_label', 'sentiment_score', 'verdict', 'context_quote', 'reported_not_creator']
    present_cols = [c for c in cols if c in df.columns]
    print(df[present_cols].to_markdown(index=False))
