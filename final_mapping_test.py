from google.cloud import bigquery
import re

client = bigquery.Client('dnd-trends-index')
sql = "SELECT concept_name, category, tags FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE category = 'Subclass' LIMIT 50"
results = client.query(sql).result()

for row in results:
    parent = "None"
    for tag in row.tags:
        match = re.match(r'^(.*)_Subclasses$', tag)
        if match:
            parent = match.group(1)
            break
    print(f"{row.concept_name} -> {parent}")
