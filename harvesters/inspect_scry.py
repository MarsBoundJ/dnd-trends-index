from google.cloud import bigquery

client = bigquery.Client()
query = "SELECT * FROM dnd_trends_categorized.trend_data_pilot WHERE search_term = 'Fighter' ORDER BY fetched_at DESC LIMIT 1"
res = list(client.query(query).result())
if res:
    print(dict(res[0]))
