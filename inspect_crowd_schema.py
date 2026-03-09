from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

print("Kickstarter Schema:")
table = client.get_table('dnd-trends-index.commercial_data.kickstarter_projects')
for schema_field in table.schema:
    print(f"{schema_field.name} ({schema_field.field_type})")

print("\nBackerKit Schema:")
table = client.get_table('dnd-trends-index.commercial_data.backerkit_projects')
for schema_field in table.schema:
    print(f"{schema_field.name} ({schema_field.field_type})")
