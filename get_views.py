from google.cloud import bigquery

client = bigquery.Client()

views = [
    "gold_data.view_social_leaderboards",
    "gold_data.view_api_leaderboards",
    "gold_data.view_fandom_leaderboards",
    "gold_data.view_wikipedia_leaderboards"
]

for view_name in views:
    print(f"--- {view_name} ---")
    try:
        view = client.get_table(view_name)
        print(view.view_query)
    except Exception as e:
        print(f"Error getting {view_name}: {e}")
    print("\n")
