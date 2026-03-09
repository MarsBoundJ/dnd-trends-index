from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

tables_to_check = ['channel_registry', 'youtube_channel_registry']
for t_id in tables_to_check:
    print(f"\n--- Checking {t_id} ---")
    try:
        t = client.get_table(f'dnd-trends-index.dnd_trends_raw.{t_id}')
        print(f"Location: {t.location}")
        print(f"Columns: {[f.name for f in t.schema]}")
    except Exception as e:
        print(f"Error: {e}")
