from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index', location='US')

query = "SELECT MAX(extraction_date) FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`"
res = list(client.query(query).result())
print(f"Latest BGG Extraction: {res[0][0]}")
