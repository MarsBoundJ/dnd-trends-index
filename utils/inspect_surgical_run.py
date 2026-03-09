from google.cloud import bigquery
import json
import datetime

def inspect():
    client = bigquery.Client()
    project = "dnd-trends-index"
    dataset = "dnd_trends_categorized"
    pilot_table = f"{project}.{dataset}.trend_data_pilot"
    
    print(f"[*] Inspecting: {pilot_table}")
    
    # 1. Total row count
    count_query = f"SELECT count(*) as count FROM `{pilot_table}`"
    total_rows = list(client.query(count_query).result())[0].count
    print(f"  [+] Total Rows: {total_rows}")
    
    # 2. Count for March 1st surgical run period (Feb - Mar 2026)
    period_query = f"SELECT count(*) as count FROM `{pilot_table}` WHERE date BETWEEN '2026-02-01' AND '2026-03-01'"
    period_rows = list(client.query(period_query).result())[0].count
    print(f"  [+] Rows (Feb-Mar 2026): {period_rows}")
    
    # 3. Check for any 'Success' from the current Playwright batch
    # The batch_id in the script starts with 'PLAYWRIGHT_PW_RETRY_'
    pw_query = f"SELECT count(*) as count FROM `{pilot_table}` WHERE batch_id LIKE 'PLAYWRIGHT_PW_RETRY_%'"
    pw_rows = list(client.query(pw_query).result())[0].count
    print(f"  [+] Rows (Current Playwright Session): {pw_rows}")
    
    # 4. Gold View Verification
    view_query = f"SELECT * FROM `dnd-trends-index.gold_data.view_api_leaderboards` LIMIT 10"
    try:
        view_results = list(client.query(view_query).result())
        print(f"\n[+] Gold View (view_api_leaderboards) rows: {len(view_results)}")
        for r in view_results:
            print(f"    - {r.category}: {r.canonical_concept} (Rank {r.rank_position})")
    except Exception as e:
        print(f"  [!] Gold View Error: {e}")

    # 5. Checkpoint SQL Proof
    proof_query = f"SELECT search_term, MAX(date) as max_date, COUNT(*) as count FROM `{pilot_table}` GROUP BY 1 ORDER BY count DESC LIMIT 15"
    print(f"\n[*] SQL Proof (Group By Keyword):")
    try:
        proof_results = list(client.query(proof_query).result())
        for r in proof_results:
            print(f"  {r.search_term}: Max {r.max_date}, Count {r.count}")
    except Exception as e:
        print(f"  [!] SQL Proof Error: {e}")

if __name__ == "__main__":
    inspect()
