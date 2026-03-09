from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

print("--- Rescuing Iuz ---")

# Step 1: Restore to Concept Library
sql_lib = """
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
SET is_active = TRUE
WHERE concept_name = 'Iuz'
"""
client.query(sql_lib).result()
print("Success: 'Iuz' set to is_active = TRUE in concept_library.")

# Step 2: Restore to AI Suggestions (so it shows back up in Admin UI)
sql_sug = """
UPDATE `dnd-trends-index.dnd_trends_raw.ai_suggestions`
SET status = 'PENDING'
WHERE concept_name = 'Iuz'
"""
client.query(sql_sug).result()
print("Success: 'Iuz' status set to 'PENDING' in ai_suggestions.")
