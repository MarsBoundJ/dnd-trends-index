from google.cloud import bigquery

client = bigquery.Client()
sql = 'SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`'
result = list(client.query(sql, location="US").result())[0][0]
print(f"Max Date: {result}")
