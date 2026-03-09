from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

print("--- Data Discovery ---")
datasets = list(client.list_datasets())
found_yt = False

for dataset in datasets:
    print(f"\nDataset: {dataset.dataset_id}")
    tables = list(client.list_tables(dataset.dataset_id))
    for table in tables:
        print(f"  - {table.table_id}")
        if 'youtube' in table.table_id.lower() or 'yt_' in table.table_id.lower():
            found_yt = True

if not found_yt:
    print("\nWARNING: No YouTube-related tables found in any dataset!")

print("\n--- Applying Schema Update ---")
alter_sql = "ALTER TABLE `dnd-trends-index.dnd_trends_categorized.concept_library` ADD COLUMN IF NOT EXISTS persona_target STRING"
try:
    client.query(alter_sql, location='US').result()
    print("SUCCESS: persona_target column added (or already exists).")
except Exception as e:
    print(f"ERROR: Failed to add persona_target: {e}")

print("\n--- Final Schema Check ---")
try:
    table = client.get_table('dnd-trends-index.dnd_trends_categorized.concept_library')
    print(f"concept_library schema: {[f.name for f in table.schema]}")
except Exception as e:
    print(f"ERROR: Failed to fetch schema: {e}")
