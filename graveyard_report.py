from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
report_sql = "SELECT class_name, subclass_name, subclass_share FROM `dnd-trends-index.gold_data.study_subclass_graveyard` WHERE subclass_share < 0.02 ORDER BY subclass_share ASC LIMIT 3"
results = client.query(report_sql).result()
for row in results:
    print(f"[{row.class_name}] {row.subclass_name}: {row.subclass_share*100:.2f}% share")
