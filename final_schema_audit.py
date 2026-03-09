from google.cloud import bigquery

client = bigquery.Client()

def info(table_id):
    t = client.get_table(table_id)
    print(f"Table: {table_id}")
    print(f"  Columns: {[f.name for f in t.schema]}")

info("dnd-trends-index.dnd_trends_categorized.trend_data_pilot")
info("dnd-trends-index.dnd_trends_categorized.expanded_search_terms")
info("dnd-trends-index.dnd_trends_categorized.concept_library")
