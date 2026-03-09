import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery

client = bigquery.Client(project="dnd-trends-index")
BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"
headers = {"Authorization": f"Bearer {BGG_TOKEN}"}

targets = [
    ("Curse of Strahd", "Curse of Strahd"),
    ("Tasha's Cauldron of Everything", "Tasha's Cauldron of Everything"),
    ("Xanathar's Guide to Everything", "Xanathar's Guide to Everything"),
    ("Monster Manual (5e)", "Monster Manual (5e)"),
    ("Dungeon Master's Guide (5e)", "Dungeon Master's Guide (5e)")
]

rows_to_insert = []
for concept_name, query in targets:
    url = f"https://boardgamegeek.com/xmlapi2/search?type=rpgitem&query={query}"
    res = requests.get(url, headers=headers)
    target_id = None
    if res.status_code == 200:
        root = ET.fromstring(res.text)
        first_item = root.find('item')
        if first_item is not None:
            target_id = first_item.get('id')
            
    if target_id:
        rows_to_insert.append({"concept_name": concept_name, "bgg_id": target_id})
        print(f"Mapped {concept_name} -> {target_id}")
    else:
        print(f"FAILED to map: {concept_name}")

if rows_to_insert:
    client.insert_rows_json("dnd-trends-index.dnd_trends_categorized.bgg_id_map", rows_to_insert)
    print(f"\nSuccessfully Inserted {len(rows_to_insert)} RPGGeek IDs into bgg_id_map.")
