from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = "SELECT subclass_name, subclass_share FROM `dnd-trends-index.gold_data.study_subclass_graveyard` ORDER BY subclass_share ASC LIMIT 5"
try:
    results = list(client.query(sql).result())
    if not results:
        print("Empty results - verifying raw concepts...")
        chk = list(client.query("SELECT COUNT(*) FROM `dnd-trends-index.gold_data.study_subclass_graveyard`").result())
        print(f"Total rows in view: {chk[0][0]}")
    else:
        for row in results:
            print(f"{row.subclass_name}: {row.subclass_share*100:.2f}%")
except Exception as e:
    print(f"Error: {e}")
