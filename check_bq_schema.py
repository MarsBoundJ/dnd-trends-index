from google.cloud import bigquery

client = bigquery.Client()

def check(table_id):
    try:
        t = client.get_table(table_id)
        print(f"Table: {table_id}")
        print(f"  Schema: {[f.name for f in t.schema]}")
    except Exception as e:
        print(f"Error checking {table_id}: {e}")

check("dnd-trends-index.dnd_trends_categorized.trend_data_pilot")
check("dnd-trends-index.silver_data.view_google_mapping")
check("dnd-trends-index.dnd_trends_categorized.concept_library")
