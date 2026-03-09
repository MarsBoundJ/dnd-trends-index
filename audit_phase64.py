from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

# 1. YouTube Registry Audit
print("--- YouTube Registry ---")
try:
    query = "SELECT channel_id, channel_name, tier FROM `dnd-trends-index.social_data.youtube_channel_registry` ORDER BY tier ASC"
    rows = list(client.query(query).result())
    print(f"Total Channels: {len(rows)}")
    for r in rows[:10]:
        print(f"Tier {r.tier}: {r.channel_name} ({r.channel_id})")
except Exception as e:
    print(f"YouTube Registry Error: {e}")

# 2. Concept Library Schema Audit
print("\n--- Concept Library Schema ---")
try:
    table = client.get_table('dnd-trends-index.dnd_trends_categorized.concept_library')
    for field in table.schema:
        print(f"{field.name}: {field.field_type}")
except Exception as e:
    print(f"Concept Library Error: {e}")
