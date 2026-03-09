from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

sql = """
SELECT table_schema, table_name, view_definition
FROM `dnd-trends-index.region-us.INFORMATION_SCHEMA.VIEWS` 
WHERE LOWER(view_definition) LIKE '%youtube_videos%'
"""

print("Running query...")
try:
    results = list(client.query(sql).result())
    if not results:
        print("No views reference youtube_videos.")
    for row in results:
        print(f"FOUND: {row.table_schema}.{row.table_name}")
except Exception as e:
    print("Error:", e)
