from google.cloud import bigquery

client = bigquery.Client()

try:
    t = client.get_table("dnd-trends-index.dnd_trends_categorized.trend_data_pilot")
    print(f"Table Type: {t.table_type}")
    if t.table_type == 'VIEW':
        print(f"View Query: {t.view_query}")
    print(f"Schema: {[f.name for f in t.schema]}")
except Exception as e:
    print(f"Error: {e}")
