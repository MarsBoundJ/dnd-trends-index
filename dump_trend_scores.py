from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

table = client.get_table('dnd-trends-index.gold_data.trend_scores')
print("--- ORIGINAL TREND SCORES ---")
print(table.view_query)
