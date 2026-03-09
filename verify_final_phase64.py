from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

print("=== Phase 64 Final Verification ===")

# 1. Persona Targets
print("\n--- Persona Target Distribution ---")
query = "SELECT persona_target, COUNT(*) as count FROM `dnd-trends-index.dnd_trends_categorized.concept_library` GROUP BY 1 ORDER BY 2 DESC"
df = client.query(query).to_dataframe()
print(df)

# 2. Milestones
print("\n--- Milestone Registry ---")
query = "SELECT COUNT(*) as count FROM `dnd-trends-index.social_data.milestone_registry`"
df = client.query(query).to_dataframe()
print(f"Total Milestones: {df.iloc[0,0]}")

# 3. YouTube Channels
print("\n--- YouTube Channel Registry ---")
query = "SELECT COUNT(*) as count FROM `dnd-trends-index.dnd_trends_raw.channel_registry`"
df = client.query(query).to_dataframe()
print(f"Total Channels: {df.iloc[0,0]}")

print("\n--- Standard Compliance ---")
# Check for uploads_playlist_id
query = "SELECT COUNT(*) as missing FROM `dnd-trends-index.dnd_trends_raw.channel_registry` WHERE uploads_playlist_id IS NULL"
missing = client.query(query).to_dataframe().iloc[0,0]
print(f"Channels missing Playlist ID: {missing}")
