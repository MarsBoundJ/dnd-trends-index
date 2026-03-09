from google.cloud import bigquery

client = bigquery.Client()

print("--- concept_library (Fighter) ---")
q = "SELECT * FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE concept_name = 'Fighter'"
results = list(client.query(q).result())
if results:
    row = results[0]
    print({k: v for k, v in row.items()})
else:
    print("Fighter not found in concept_library.")
