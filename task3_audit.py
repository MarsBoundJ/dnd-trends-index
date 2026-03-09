from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

# 1. Milestone Seeding (Surgical)
print("--- Seeding Milestones ---")
ms_sql = """
INSERT INTO `dnd-trends-index.social_data.milestone_registry` (event_name, event_date, category_affected, impact_level, description)
VALUES 
('D&D Player\\'s Handbook (2024) Launch', '2024-09-03', 'Class', 'CRITICAL', 'Official release of the new core rulebook.'),
('D&D Dungeon Master\\'s Guide (2024) Launch', '2024-11-12', 'Setting', 'HIGH', 'Official release of the new DM guide.'),
('D&D Monster Manual (2025) Release', '2025-02-18', 'Monster', 'HIGH', 'Official release of the new creature supplement.'),
('Critical Role Campaign 3 Premiere', '2021-10-21', 'Actual Play', 'CRITICAL', 'Start of the third major CR campaign.')
"""
try:
    client.query(ms_sql).result()
    print("SUCCESS: Milestones seeded.")
except Exception as e:
    print(f"FAILED: Milestone seeding error: {e}")

# 2. YouTube Channel Audit
print("\n--- YouTube Channel Audit ---")
registry_id = 'dnd-trends-index.dnd_trends_raw.channel_registry'
try:
    query = f"SELECT channel_id, channel_name, tier FROM `{registry_id}` ORDER BY tier ASC"
    rows = list(client.query(query).result())
    print(f"Total Channels: {len(rows)}")
    for r in rows:
        print(f"Tier {r.tier}: {r.channel_name} ({r.channel_id})")
except Exception as e:
    print(f"FAILED: Channel audit error: {e}")
