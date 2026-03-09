from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

# Query for archived items
print("--- Recently Archived Items ---")
query_archived = """
SELECT concept_name, category, last_processed_at 
FROM `dnd-trends-index.dnd_trends_categorized.concept_library` 
WHERE is_active = FALSE 
ORDER BY last_processed_at DESC 
LIMIT 10
"""
results = client.query(query_archived).result()
for row in results:
    print(f"Concept: {row.concept_name} | Category: {row.category} | Timestamp: {row.last_processed_at}")

# Archive Space clown
print("\n--- Archiving Space clown ---")
query_space_clown = """
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library` 
SET is_active = FALSE 
WHERE concept_name = 'Space clown'
"""
client.query(query_space_clown).result()
print("Success: 'Space clown' has been archived.")
