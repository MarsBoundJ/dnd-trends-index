import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery

client = bigquery.Client(project="dnd-trends-index")

targets = [
    ("D&D Player's Handbook (2024)", "415949"),
    ("Shadowdark RPG", "390918"),
    ("Daggerheart", "420367"),
    ("Draw Steel (MCDM)", "437817")
]

rows_to_insert = []
for concept_name, target_id in targets:
    rows_to_insert.append({"concept_name": concept_name, "bgg_id": target_id})
    print(f"Mapped {concept_name} -> {target_id}")

client.insert_rows_json("dnd-trends-index.dnd_trends_categorized.bgg_id_map", rows_to_insert)
print(f"Inserted {len(rows_to_insert)} IDs into bgg_id_map.")
