from google.cloud import bigquery
from google.api_core.exceptions import NotFound

client = bigquery.Client(project='dnd-trends-index')

try:
    print("dnd_trends_raw.kickstarter_daily Schema:")
    table = client.get_table('dnd-trends-index.dnd_trends_raw.kickstarter_daily')
    for schema_field in table.schema:
        print(f"{schema_field.name} ({schema_field.field_type})")
except NotFound:
    print("Table not found in dnd_trends_raw.")

try:
    print("\ncommercial_data.kickstarter_daily Schema:")
    table = client.get_table('dnd-trends-index.commercial_data.kickstarter_daily')
    for schema_field in table.schema:
        print(f"{schema_field.name} ({schema_field.field_type})")
except NotFound:
    print("Table not found in commercial_data.")

try:
    print("\ndnd_trends_raw.backerkit_daily Schema:")
    table = client.get_table('dnd-trends-index.dnd_trends_raw.backerkit_daily')
    for schema_field in table.schema:
        print(f"{schema_field.name} ({schema_field.field_type})")
except NotFound:
    print("Table not found in dnd_trends_raw.")
