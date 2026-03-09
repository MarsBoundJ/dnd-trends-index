from google.cloud import bigquery

client = bigquery.Client()

def check_schema(dataset, table):
    try:
        t = client.get_table(f"dnd-trends-index.{dataset}.{table}")
        print(f"--- {dataset}.{table} ---")
        print([f.name for f in t.schema])
    except Exception as e:
        print(f"--- {dataset}.{table} --- NOT FOUND: {e}")

check_schema('dnd_trends_categorized', 'trend_data_pilot')
check_schema('dnd_trends_raw', 'trend_data_pilot')
check_schema('dnd_trends_categorized', 'expanded_search_terms')
check_schema('silver_data', 'view_google_mapping')
