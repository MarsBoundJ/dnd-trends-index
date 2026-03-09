import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery

BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"
headers = {"Authorization": f"Bearer {BGG_TOKEN}"}
client = bigquery.Client(project="dnd-trends-index")

targets = [
    ("D&D Player's Handbook (2024)", "Player's Handbook (2024)", "415949"),
    ("Shadowdark RPG", "Shadowdark RPG", "390918"),
    ("Daggerheart", "Daggerheart", "420367"),
    ("Draw Steel (MCDM)", "Draw Steel", "437817")
]

rows_to_insert = []
for concept_name, query, fallback_id in targets:
    url = f"https://boardgamegeek.com/xmlapi2/search?type=rpgitem&query={query}"
    res = requests.get(url, headers=headers)
    target_id = fallback_id
    if res.status_code == 200:
        root = ET.fromstring(res.text)
        first_item = root.find('item')
        if first_item is not None:
            target_id = first_item.get('id')
    
    rows_to_insert.append({"concept_name": concept_name, "bgg_id": target_id})
    print(f"Mapped {concept_name} -> {target_id}")

if rows_to_insert:
    # Delete first
    names = ", ".join([f"'{r['concept_name']}'" for r in rows_to_insert])
    client.query(f"DELETE FROM `dnd-trends-index.dnd_trends_categorized.bgg_id_map` WHERE concept_name IN ({names})").result()
    client.insert_rows_json("dnd-trends-index.dnd_trends_categorized.bgg_id_map", rows_to_insert)
    print(f"Inserted {len(rows_to_insert)} IDs into bgg_id_map.")
