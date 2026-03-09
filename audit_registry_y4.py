from google.cloud import bigquery

client = bigquery.Client()
query = "SELECT * FROM `dnd-trends-index.dnd_trends_raw.channel_registry`"
df = client.query(query).to_dataframe()

print("--- Channel Registry Audit ---")
print(df.to_string(index=False))

invalid_ids = df[~df['channel_id'].str.startswith('UC', na=False)]
if not invalid_ids.empty:
    print("\n[WARNING] Found invalid/non-UC IDs:")
    print(invalid_ids[['channel_name', 'channel_id']])
else:
    print("\n[SUCCESS] All IDs follow the UC-prefix standard.")
