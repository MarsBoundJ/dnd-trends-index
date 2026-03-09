from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

# Check norm_buy for the latest date in rpggeek_product_stats
sql = """
SELECT keyword, score_buy, date 
FROM `dnd-trends-index.silver_data.norm_buy` 
WHERE date = '2026-03-02'
LIMIT 10
"""
print("--- norm_buy on 2026-03-02 ---")
try:
    results = [dict(r) for r in client.query(sql).result()]
    if results:
        for r in results:
            print(r)
    else:
        print("No results for 2026-03-02")
except Exception as e:
    print(f"Error: {e}")

# Check any data for a set of keywords from rpggeek
sql2 = """
SELECT DISTINCT concept_name FROM `dnd-trends-index.dnd_trends_raw.rpggeek_product_stats` LIMIT 10
"""
keywords = [r.concept_name for r in client.query(sql2).result()]
print(f"\n--- RPGGeek Keywords: {keywords} ---")

if keywords:
    sql3 = f"""
    SELECT keyword, date FROM `dnd-trends-index.silver_data.norm_buy`
    WHERE keyword IN {tuple(keywords)}
    ORDER BY date DESC LIMIT 10
    """
    try:
        results = [dict(r) for r in client.query(sql3).result()]
        for r in results:
            print(r)
    except Exception as e:
        print(f"Error checking keywords: {e}")
