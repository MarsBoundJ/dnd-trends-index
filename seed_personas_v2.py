from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

# Seeding Logic
# Using a slightly refined list of categories
seeding_sql = """
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
SET persona_target = CASE
    WHEN category IN ('NPC', 'Monster', 'Villain', 'Adventure', 'Setting', 'Lore', 'Campaign', 'Villain/Antagonist') THEN 'DM'
    WHEN category IN ('Spell', 'Class', 'Subclass', 'Feat', 'Equipment', 'PC', 'Racial trait', 'Item') THEN 'Player'
    WHEN category IN ('Mechanic', 'Rulebook', 'TTRPG System', 'Influencer', 'Community', 'Publisher', 'Actual Play') THEN 'Shared'
    ELSE 'Shared'
END
WHERE 1=1
"""
print("--- Seeding persona_target ---")
job = client.query(seeding_sql)
job.result()
print(f"Seeding complete. {job.num_dml_affected_rows} rows updated.")
