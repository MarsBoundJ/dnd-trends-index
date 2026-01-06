from google.cloud import bigquery

def audit_tables():
    client = bigquery.Client()
    datasets = list(client.list_datasets())
    for ds in datasets:
        dataset_id = ds.dataset_id
        dataset = client.get_dataset(dataset_id)
        print(f"Dataset: {dataset_id} | Location: {dataset.location}")
        tables = list(client.list_tables(dataset))
        for t in tables:
            print(f"  - Table: {t.table_id}")

if __name__ == "__main__":
    audit_tables()
