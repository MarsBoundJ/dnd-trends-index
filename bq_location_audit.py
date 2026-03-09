from google.cloud import bigquery

client = bigquery.Client()
ds_id = "dnd_trends_raw"
ds = client.get_dataset(ds_id)
print(f"Dataset {ds_id} Location: {ds.location}")

tables = list(client.list_tables(ds.reference))
for t in tables:
    print(f"  - {t.table_id}")
