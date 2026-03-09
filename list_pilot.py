from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

query = "SELECT search_term FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` WHERE is_pilot = TRUE LIMIT 10"
results = client.query(query).result()
for row in results:
    print(f"- {row.search_term}")
