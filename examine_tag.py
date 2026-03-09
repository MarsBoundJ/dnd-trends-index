from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = "SELECT concept_name, tags FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE concept_name = 'Bladesinger 5e'"
results = client.query(sql).result()
for row in results:
    print(f"Name: {row.concept_name}")
    print(f"Tags Type: {type(row.tags)}")
    print(f"Tags Content: {row.tags}")
