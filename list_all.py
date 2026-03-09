from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

print("Commercial Data Tables:")
for t in client.list_tables('dnd-trends-index.commercial_data'):
    print(t.table_id)

print("\nDnD Trends Raw Tables:")
for t in client.list_tables('dnd-trends-index.dnd_trends_raw'):
    print(t.table_id)
