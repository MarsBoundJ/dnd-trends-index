from google.cloud import bigquery
import os

client = bigquery.Client()

views = [
    "gold_data.deep_dive_metrics",
    "silver_data.view_fandom_mapping"
]

os.makedirs("underlying_defs", exist_ok=True)

for view_name in views:
    try:
        view = client.get_table(view_name)
        if view.table_type == "VIEW":
            filename = f"underlying_defs/{view_name.replace('.', '_')}.sql"
            with open(filename, "w") as f:
                f.write(view.view_query)
            print(f"Saved {view_name} to {filename}")
        else:
            print(f"{view_name} is a {view.table_type}, not a VIEW.")
    except Exception as e:
        print(f"Error getting {view_name}: {e}")
