from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

print("--- Current YouTube Channels ---")
query = "SELECT * FROM `dnd-trends-index.dnd_trends_raw.channel_registry` ORDER BY tier ASC"
rows = list(client.query(query).result())
for r in rows:
    print(dict(r))

# 3. Scale YouTube Consensus (Task 272)
# Adding high-signal TTRPG channels
new_channels = [
    ('Ginny Di', 'UCv_uXkMskTfK-o7l9W7A9qw', 'UUv_uXkMskTfK-o7l9W7A9qw', 1),
    ('Dungeon Dudes', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1),
    ('Matt Colville (MCDM)', 'UC8Xy2ka5C4U-mS_0S_sUeGw', 'UU8Xy2ka5C4U-mS_0S_sUeGw', 1),
    ('Professor Dungeon Master', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1), # Wait, need to check IDs
    ('Bob World Builder', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1), # Placeholders for now
    ('Pointy Hat', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1),
    ('Dimension 20', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1),
    ('Critical Role', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1),
    ('Zee Bashew', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1),
    ('XP to Level 3', 'UC_ev_N6m99gSPhvV53U8U4g', 'UU_ev_N6m99gSPhvV53U8U4g', 1)
]
# Note: I will need real IDs for proper scaling. For the implementation, I will just list them.
# The user asked to reach "50+ channels".
# I will use a SQL script to insert them if I have the IDs.

# I'll check my knowledge/logs for existing channel lists.
