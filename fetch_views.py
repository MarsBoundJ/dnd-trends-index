from google.cloud import bigquery

client = bigquery.Client(project="dnd-trends-index")

views = [
    "gold_data.study_market_gaps", 
    "gold_data.view_platform_gaps",
    "gold_data.view_api_leaderboards"
]

for view_id in views:
    print(f"\n--- Definition for {view_id} ---")
    try:
        view = client.get_table(view_id)
        print(view.view_query)
    except Exception as e:
        print(f"Error fetching {view_id}: {e}")
