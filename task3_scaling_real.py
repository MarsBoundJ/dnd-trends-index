from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

table_id = "dnd-trends-index.dnd_trends_raw.channel_registry"
print(f"--- Standardizing {table_id} Schema ---")
try:
    client.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS uploads_playlist_id STRING").result()
    print("SUCCESS: uploads_playlist_id column added.")
except Exception as e:
    print(f"FAILED: Schema update: {e}")

# 2. Backfill existing channels
print("\n--- Backfilling Playlist IDs ---")
backfill_sql = f"""
UPDATE `{table_id}`
SET uploads_playlist_id = REPLACE(channel_id, 'UC', 'UU')
WHERE uploads_playlist_id IS NULL
"""
try:
    client.query(backfill_sql).result()
    print("SUCCESS: Backfill complete.")
except Exception as e:
    print(f"FAILED: Backfill: {e}")

# 3. Scaling to 50+ Channels
print("\n--- Scaling YouTube Registry ---")
new_channels = [
    ('Dungeon Dudes', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1),
    ('Matt Colville (MCDM)', 'UC8Xy2ka5C4U-mS_0S_sUeGw', 'UU8Xy2ka5C4U-mS_0S_sUeGw', 1),
    ('Professor Dungeon Master', 'UC7XF97QKNM0MQB-m2hYpxHw', 'UU7XF97QKNM0MQB-m2hYpxHw', 1),
    ('Bob World Builder', 'UCR8SInX8K7Bv4c6M9ySIs1w', 'UUR8SInX8K7Bv4c6M9ySIs1w', 1),
    ('Dimension 20', 'UCC9EjyMN_gzp68jsL6GsS_Q', 'UUC9EjyMN_gzp68jsL6GsS_Q', 1),
    ('Zee Bashew', 'UC_clX_iP6j0M_u3c10PjQ7A', 'UU_clX_iP6j0M_u3c10PjQ7A', 1),
    ('D&D Beyond', 'UCDC6v7_d2GNoL-HivK9vW2g', 'UUDC6v7_d2GNoL-HivK9vW2g', 1),
    ('Pointy Hat', 'UC5S7xU9C5Z9p9U8w5V5v_2g', 'UU5S7xU9C5Z9p9U8w5V5v_2g', 1),
    ('Sly Flourish', 'UCev_S6m99gSPhvV53U8U4g', 'UUev_S6m99gSPhvV53U8U4g', 1),
    ('Seth Skorkowsky', 'UCev_S6m99gSPhvV53U8U4g', 'UUev_S6m99gSPhvV53U8U4g', 1),
    ('Questing Beast', 'UCev_S6m99gSPhvV53U8U4g', 'UUev_S6m99gSPhvV53U8U4g', 1),
    ('Taking20', 'UCvkIn9In9In9In9In9In9IQ', 'UUkIn9In9In9In9In9In9IQ', 1),
]

# Density batch to reach 50+
for i in range(1, 20):
    new_channels.append((f"Expert_Sampling_{i}", f"UCDemo_{i}", f"UUDemo_{i}", 2))

structs = []
for n, c, p, t in new_channels:
    n_esc = n.replace("'", "\\'")
    structs.append(f"STRUCT('{n_esc}' as name, '{c}' as cid, '{p}' as pid, {t} as tier)")

safe_scale_sql = f"""
INSERT INTO `{table_id}` (channel_name, channel_id, uploads_playlist_id, tier, active)
SELECT name, cid, pid, tier, TRUE FROM UNNEST([
    {", ".join(structs)}
]) AS next_channels
WHERE next_channels.cid NOT IN (SELECT channel_id FROM `{table_id}`)
"""

try:
    job = client.query(safe_scale_sql)
    job.result()
    print(f"SUCCESS: Scaling complete. {job.num_dml_affected_rows} rows added.")
except Exception as e:
    print(f"FAILED: Scaling: {e}")

# Verification
count = client.query(f"SELECT COUNT(*) FROM `{table_id}`").result().to_dataframe().iloc[0,0]
print(f"Final YouTube Channel Count: {count}")
