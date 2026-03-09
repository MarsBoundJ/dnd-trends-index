from google.cloud import bigquery

client = bigquery.Client()

def check_max_google():
    print("--- Google Max Date Check ---")
    sql = "SELECT MAX(date) as max_date FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`"
    try:
        query_job = client.query(sql, location="US")
        results = query_job.to_dataframe()
        print(f"MAX Date in trend_data_pilot: {results['max_date'][0]}")
    except Exception as e:
        print(f"Error checking max google: {e}")

def debug_amazon_map():
    print("\n--- Amazon Map Debug ---")
    MAP_TABLE = "dnd-trends-index.dnd_trends_categorized.amazon_asin_map"
    sql = f"SELECT * FROM `{MAP_TABLE}`"
    try:
        query_job = client.query(sql, location="US")
        rows = list(query_job.result())
        print(f"Number of rows found by Client.query: {len(rows)}")
        for i, row in enumerate(rows):
            print(f"Row {i}: {row.concept_name} | {row.asin}")
    except Exception as e:
        print(f"Error debugging amazon map: {e}")

if __name__ == "__main__":
    check_max_google()
    debug_amazon_map()
