from google.cloud import bigquery
import json

client = bigquery.Client()

tables = [
    "dnd-trends-index.dnd_trends_categorized.trend_data_pilot",
    "dnd-trends-index.dnd_trends_categorized.expanded_search_terms",
    "dnd-trends-index.dnd_trends_categorized.concept_library"
]

report = {}
for table_id in tables:
    try:
        t = client.get_table(table_id)
        report[table_id] = [f.name for f in t.schema]
    except Exception as e:
        report[table_id] = f"Error: {e}"

with open("schema_report.json", "w") as f:
    json.dump(report, f, indent=2)

print("Schema report written to schema_report.json")
