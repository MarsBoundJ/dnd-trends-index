from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
client.query("CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.rpggeek_product_stats` (date DATE, concept_name STRING, owned_count INT64, quality_score FLOAT64)").result()
print("Table created.")
