from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

print("--- Current Categories ---")
query = "SELECT DISTINCT category FROM `dnd-trends-index.dnd_trends_categorized.concept_library`"
results = client.query(query).result()
for row in results:
    print(f"- {row.category}")

print("\n--- Initializing Phase 64 ---")

# Task 1: Add persona_target column
print("Adding persona_target column...")
try:
    client.query("ALTER TABLE `dnd-trends-index.dnd_trends_categorized.concept_library` ADD COLUMN IF NOT EXISTS persona_target STRING").result()
    print("Column added or already exists.")
except Exception as e:
    print(f"Error adding column: {e}")

# Apply persona_target mapping
print("Updating persona_target mappings...")
mapping_sql = """
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
SET persona_target = CASE
    WHEN category IN ('Monster', 'Villain', 'Location', 'Setting', 'NPC', 'Faction', 'Deity', 'Villain/Antagonist') THEN 'DM'
    WHEN category IN ('Class', 'Subclass', 'Feat', 'Species & Lineage', 'Background', 'PC', 'Equipment', 'Item', 'Racial trait') THEN 'PLAYER'
    WHEN category IN ('Mechanic', 'Spell', 'MagicItem', 'Rulebooks', 'Rulebook') THEN 'SHARED'
    WHEN category IN ('YouTube', 'Influencer', 'Actual Play', 'Convention', 'VTT Platform', 'Publisher', 'Community') THEN 'META'
    ELSE 'SHARED'
END
WHERE 1=1
"""
client.query(mapping_sql).result()
print("Persona mapping complete.")

# Task 2: Create gold_data.event_milestones
print("Creating gold_data.event_milestones...")
create_sql = """
CREATE TABLE IF NOT EXISTS `dnd-trends-index.gold_data.event_milestones` (
    event_name STRING,
    event_date DATE,
    event_type STRING
)
"""
client.query(create_sql).result()

# Seed milestones
print("Seeding milestones...")
seed_sql = """
INSERT INTO `dnd-trends-index.gold_data.event_milestones` (event_name, event_date, event_type)
SELECT * FROM UNNEST([
    STRUCT('2014 PHB Release' as name, DATE '2014-08-19' as d, 'Release' as t),
    STRUCT('2024 PHB Release' as name, DATE '2024-09-17' as d, 'Release' as t),
    STRUCT('2024 DMG Release' as name, DATE '2024-11-12' as d, 'Release' as t),
    STRUCT('OGL Controversy Start' as name, DATE '2023-01-05' as d, 'Controversy' as t),
    STRUCT('BG3 Full Release' as name, DATE '2023-08-03' as d, 'Media' as t)
]) as seed
WHERE NOT EXISTS (
    SELECT 1 FROM `dnd-trends-index.gold_data.event_milestones` m 
    WHERE m.event_name = seed.name
)
"""
client.query(seed_sql).result()
print("Milestone seeding complete.")
