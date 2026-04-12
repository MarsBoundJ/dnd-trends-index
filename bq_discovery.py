from google.cloud import bigquery

client = bigquery.Client(project="dnd-trends-index")
datasets = ["gold_data", "silver_data", "dnd_trends_categorized", "dnd_trends_raw"]

with open("/tmp/bq_tables.txt", "w") as f:
    for dataset_id in datasets:
        f.write(f"\n--- {dataset_id} ---\n")
        dataset_ref = client.dataset(dataset_id)
        tables = client.list_tables(dataset_ref)
        for table in tables:
            f.write(f"{table.table_id} ({table.table_type})\n")
print("Done writing to /tmp/bq_tables.txt")
