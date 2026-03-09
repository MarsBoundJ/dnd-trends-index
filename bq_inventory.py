from google.cloud import bigquery

client = bigquery.Client()
datasets = list(client.list_datasets())
print(f"Total Datasets: {len(datasets)}")

for ds in datasets:
    print(f"\nDataset: {ds.dataset_id}")
    tables = list(client.list_tables(ds.reference))
    for table in tables:
        t_ref = client.get_table(table.reference)
        print(f"  - {table.table_id}: {t_ref.num_rows} rows")
