from google.cloud import bigquery

client = bigquery.Client()
table_id = "dnd-trends-index.dnd_trends_categorized.trend_data_pilot"

try:
    table = client.get_table(table_id)
    print(f"Schema for {table_id}:")
    for field in table.schema:
        print(f"- {field.name} ({field.field_type})")
except Exception as e:
    print(f"Error getting table: {e}")
