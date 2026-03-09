import requests
import xml.etree.ElementTree as ET
import time
from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

target_books = [
    "Tasha's Cauldron of Everything",
    "Xanathar's Guide to Everything",
    "Vecna: Eve of Ruin",
    "Phandelver and Below",
    "Volo's Guide to Monsters",
    "Mordenkainen's Tome of Foes",
    "Monsters of the Multiverse",
    "Fizban's Treasury of Dragons",
    "Glory of the Giants",
    "The Book of Many Things",
    "Sword Coast Adventurer's Guide",
    "Tal'Dorei Campaign Setting",
    "Explorer's Guide to Wildemount",
    "Guildmasters' Guide to Ravnica",
    "Mythic Odysseys of Theros",
    "Spelljammer: Adventures in Space",
    "Shadow of the Dragon Queen",
    "Van Richten's Guide to Ravenloft",
    "Radiant Citadel",
    "Keys from the Golden Vault",
    "Candlekeep Mysteries"
]

found_ids = []

for book in target_books:
    # simplify query
    search_term = book.split(":")[0].split("'s")[0].replace(" ", "+")
    url = f"https://boardgamegeek.com/xmlapi2/search?query={search_term}&type=rpgitem"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            items = root.findall("item")
            for item in items:
                name_elem = item.find("name")
                if name_elem is not None:
                    name_val = name_elem.attrib.get('value', '')
                    book_clean = book.replace("'", "").lower()
                    name_clean = name_val.replace("'", "").lower()
                    # Check if all major words are in the title
                    words = [w for w in book_clean.split() if len(w) > 3]
                    if all(w in name_clean for w in words):
                        bgg_id = int(item.attrib.get('id'))
                        found_ids.append((book, bgg_id))
                        print(f"✅ Found {book} -> [{name_val}]: {bgg_id}")
                        break
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error searching {book}: {e}")

print(f"\nTotal Confirmed: {len(found_ids)}")

if len(found_ids) > 0:
    rows = [{"concept_name": b, "bgg_id": i} for b, i in found_ids]
    names = [f"'{b.replace(chr(39), chr(92)+chr(39))}'" for b, i in found_ids]
    del_query = f"DELETE FROM `dnd-trends-index.dnd_trends_categorized.bgg_id_map` WHERE concept_name IN ({','.join(names)})"
    client.query(del_query).result()
    errors = client.insert_rows_json("dnd-trends-index.dnd_trends_categorized.bgg_id_map", rows)
    if errors:
        print(f"Insert errors: {errors}")
    else:
        print("Successfully inserted IDs into mapping table.")
