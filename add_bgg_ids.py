import requests
import xml.etree.ElementTree as ET
import time
from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

target_books = [
    "Tasha's Cauldron of Everything",
    "Xanathar's Guide to Everything",
    "Vecna: Eve of Ruin",
    "Phandelver and Below: The Shattered Obelisk",
    "Volo's Guide to Monsters",
    "Mordenkainen's Tome of Foes",
    "Mordenkainen Presents: Monsters of the Multiverse",
    "Fizban's Treasury of Dragons",
    "Bigby Presents: Glory of the Giants",
    "The Book of Many Things",
    "Sword Coast Adventurer's Guide",
    "Tal'Dorei Campaign Setting Reborn",
    "Explorer's Guide to Wildemount",
    "Guildmasters' Guide to Ravnica",
    "Mythic Odysseys of Theros",
    "Spelljammer: Adventures in Space",
    "Planescape: Adventures in the Multiverse",
    "Spelljammer",
    "Dragonlance: Shadow of the Dragon Queen",
    "Van Richten's Guide to Ravenloft",
    "Journeys through the Radiant Citadel",
    "Keys from the Golden Vault",
    "Candlekeep Mysteries"
]

found_ids = []

for book in target_books:
    url = f"https://boardgamegeek.com/xmlapi2/search?query={book.replace(' ', '+')}&type=rpgitem,boardgame,boardgameexpansion"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            items = root.findall("item")
            for item in items:
                name_elem = item.find("name")
                if name_elem is not None:
                    name_val = name_elem.attrib.get('value', '')
                    if book.lower() in name_val.lower() or name_val.lower() in book.lower():
                        bgg_id = int(item.attrib.get('id'))
                        found_ids.append((book, bgg_id))
                        print(f"✅ Found {book}: {bgg_id}")
                        break
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error searching {book}: {e}")

print(f"\nTotal Confirmed: {len(found_ids)}")

if len(found_ids) > 0:
    rows = [{"concept_name": b, "bgg_id": i} for b, i in found_ids]
    
    # Just delete these specific concepts if they exist to avoid dupes, then insert
    names = [f"'{b.replace(chr(39), chr(92)+chr(39))}'" for b, i in found_ids]
    del_query = f"DELETE FROM `dnd-trends-index.dnd_trends_categorized.bgg_id_map` WHERE concept_name IN ({','.join(names)})"
    
    try:
        client.query(del_query).result()
        errors = client.insert_rows_json("dnd-trends-index.dnd_trends_categorized.bgg_id_map", rows)
        if errors:
            print(f"Insert errors: {errors}")
        else:
            print("Successfully inserted IDs into mapping table.")
    except Exception as e:
        print(f"BQ error: {e}")
