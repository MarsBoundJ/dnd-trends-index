from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
query = client.query("SELECT * FROM `dnd-trends-index.gold_data.event_milestones` LIMIT 5")
for row in query.result():
    print(dict(row))
