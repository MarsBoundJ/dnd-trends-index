from google.cloud import bigquery
import os

client = bigquery.Client()

views = [
    "gold_data.view_social_leaderboards",
    "gold_data.view_api_leaderboards",
    "gold_data.view_fandom_leaderboards",
    "gold_data.view_wikipedia_leaderboards"
]

os.makedirs("view_defs", exist_ok=True)

for view_name in views:
    try:
        view = client.get_table(view_name)
        filename = f"view_defs/{view_name.replace('.', '_')}.sql"
        with open(filename, "w") as f:
            f.write(view.view_query)
        print(f"Saved {view_name} to {filename}")
    except Exception as e:
        print(f"Error getting {view_name}: {e}")
