from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

query = "SELECT * FROM `dnd-trends-index.dnd_trends_categorized.bgg_id_map` LIMIT 10"
for row in client.query(query).result():
    print(f"{row.concept_name}: {row.bgg_id}")

