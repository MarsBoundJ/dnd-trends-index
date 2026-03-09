from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

# Seeding Logic
seeding_sql = """
UPDATE `dnd_trends_categorized.concept_library`
SET persona_target = CASE
    WHEN category IN ('NPC', 'Monster', 'Villain', 'Adventure', 'Setting', 'Lore', 'Campaign', 'Villain/Antagonist') THEN 'DM'
    WHEN category IN ('Spell', 'Class', 'Subclass', 'Feat', 'Equipment', 'PC', 'Racial trait', 'Item') THEN 'Player'
    WHEN category IN ('Mechanic', 'Rulebook', 'TTRPG System', 'Influencer', 'Community', 'Publisher', 'Actual Play') THEN 'Shared'
    ELSE 'Shared'
END
WHERE 1=1
"""
print("--- Seeding persona_target (No project prefix) ---")
try:
    job = client.query(seeding_sql)
    job.result()
    print(f"Seeding complete. {job.num_dml_affected_rows} rows updated.")
except Exception as e:
    print(f"Seeding failed: {e}")

# Verification
print("\n--- Sample Check ---")
check_query = "SELECT concept_name, category, persona_target FROM `dnd_trends_categorized.concept_library` LIMIT 5"
for r in client.query(check_query).result():
    print(f"{r.concept_name} | {r.category} | {r.persona_target}")
