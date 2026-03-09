from google.cloud import bigquery
import json

client = bigquery.Client()

def check_things():
    # 1. Check progress
    q1 = "SELECT count(distinct search_term) as c FROM dnd_trends_categorized.trend_data_pilot WHERE batch_id LIKE 'GHOST_WALK_%'"
    res1 = list(client.query(q1).result())
    print(f"Processed batch count: {res1[0]['c']}")

    # 2. Check view definition
    q2 = "SELECT view_definition FROM `dnd-trends-index.gold_data.INFORMATION_SCHEMA.VIEWS` WHERE table_name = 'view_api_leaderboards'"
    res2 = list(client.query(q2).result())
    if res2:
        with open("view_def.sql", "w") as f:
            f.write(res2[0]['view_definition'])
        print("Wrote view_def.sql")

if __name__ == '__main__':
    check_things()
