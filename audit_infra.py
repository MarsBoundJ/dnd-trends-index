from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

print("=== PROJECT INFRASTRUCTURE AUDIT ===")

# 1. Search for YouTube Registry
print("\nSearching for YouTube-related tables...")
datasets = list(client.list_datasets())
found_any = False
for ds in datasets:
    tables = list(client.list_tables(ds.dataset_id))
    for t in tables:
        if 'youtube' in t.table_id.lower() or 'yt_' in t.table_id.lower() or 'channel' in t.table_id.lower():
            print(f"FOUND: {ds.dataset_id}.{t.table_id}")
            found_any = True

if not found_any:
    print("WARNING: No YouTube tables found! Checking social_data tables explicitly...")
    try:
        sd_tables = [t.table_id for t in client.list_tables('social_data')]
        print(f"social_data tables: {sd_tables}")
    except:
        print("social_data dataset not found.")

# 2. Check and Create Milestone Registry
print("\nChecking Milestone Registry...")
ms_id = 'dnd-trends-index.social_data.milestone_registry'
try:
    client.get_table(ms_id)
    print("milestone_registry: EXISTS")
except:
    print("milestone_registry: MISSING. Creating now...")
    schema = [
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("category_affected", "STRING"),
        bigquery.SchemaField("impact_level", "STRING"),
        bigquery.SchemaField("description", "STRING"),
    ]
    table = bigquery.Table(ms_id, schema=schema)
    client.create_table(table)
    print("milestone_registry: CREATED")

# 3. Verify concept_library persona_target
print("\nVerifying concept_library schema...")
cl_id = 'dnd-trends-index.dnd_trends_categorized.concept_library'
table = client.get_table(cl_id)
cols = [f.name for f in table.schema]
print(f"Columns: {cols}")
if 'persona_target' in cols:
    print("persona_target: FOUND")
else:
    print("persona_target: MISSING - Re-applying ALTER TABLE...")
    client.query(f"ALTER TABLE `{cl_id}` ADD COLUMN IF NOT EXISTS persona_target STRING").result()
    print("persona_target: ADDED")
