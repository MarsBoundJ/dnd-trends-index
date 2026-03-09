from google.cloud import bigquery

client = bigquery.Client()

tables = [
    "dnd-trends-index.dnd_trends_categorized.trend_data_pilot",
    "dnd-trends-index.dnd_trends_categorized.expanded_search_terms",
    "dnd-trends-index.dnd_trends_categorized.concept_library"
]

for table_id in tables:
    try:
        t = client.get_table(table_id)
        print(f"--- {table_id} ---")
        print(f"Columns: {[f.name for f in t.schema]}")
    except Exception as e:
        print(f"Error reading {table_id}: {e}")
