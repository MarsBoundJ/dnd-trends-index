from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
sql = """
UPDATE `dnd-trends-index.dnd_trends_raw.ai_suggestions`
SET suggested_category = 'PC'
WHERE concept_name IN ('Mollymauk Tealeaf', 'Caleb Widogast')
"""
client.query(sql).result()
print('Updated CR characters to PC category.')
