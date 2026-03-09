from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

# Query Categories
print("--- Current Categories ---")
query = "SELECT DISTINCT category FROM `dnd-trends-index.dnd_trends_categorized.concept_library`"
results = client.query(query).result()
categories = [r.category for r in results]
print(categories)

# Seeding Logic
seeding_sql = """
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
SET persona_target = CASE
    WHEN category IN ('NPC', 'Monster', 'Villain', 'Adventure', 'Setting', 'Lore', 'Campaign') THEN 'DM'
    WHEN category IN ('Spell', 'Class', 'Subclass', 'Feat', 'Equipment', 'PC', 'Racial trait') THEN 'Player'
    WHEN category IN ('Mechanic', 'Rulebook', 'TTRPG System', 'Influencer', 'Community') THEN 'Shared'
    ELSE 'Shared'
END
WHERE persona_target IS NULL
"""
print("\n--- Seeding persona_target ---")
client.query(seeding_sql).result()
print("Seeding complete.")
