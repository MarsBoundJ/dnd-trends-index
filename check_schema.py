from google.cloud import bigquery
client = bigquery.Client()
table = client.get_table("dnd-trends-index.dnd_trends_categorized.concept_library")
print([f.name for f in table.schema])
