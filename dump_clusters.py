from google.cloud import bigquery

client = bigquery.Client(project="dnd-trends-index")

query = "SELECT * FROM `dnd-trends-index.dnd_trends_categorized.aesthetic_clusters`"
results = client.query(query).to_dataframe()

print(results.to_string())
