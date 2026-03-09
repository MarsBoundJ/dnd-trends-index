from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

print("--- BigQuery Dataset/Table Discovery ---")
datasets = list(client.list_datasets())
print(f"Found {len(datasets)} datasets.")

for dataset in datasets:
    print(f"\nDataset: {dataset.dataset_id}")
    try:
        tables = list(client.list_tables(dataset.dataset_id))
        for table in tables:
            print(f"  - {table.table_id}")
    except Exception as e:
        print(f"  Error listing tables: {e}")
