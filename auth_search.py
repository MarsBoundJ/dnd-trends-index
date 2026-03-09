import requests
import xml.etree.ElementTree as ET

BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"
headers = {"Authorization": f"Bearer {BGG_TOKEN}"}

targets = [
    ("D&D Handbook 2024", "Player's Handbook (2024)"),
    ("Shadowdark RPG", "Shadowdark RPG"),
    ("Daggerheart", "Daggerheart"),
    ("Draw Steel", "Draw Steel")
]

for t, query in targets:
    url = f"https://boardgamegeek.com/xmlapi2/search?type=rpgitem&query={query}"
    res = requests.get(url, headers=headers)
    print(f"\n--- {t} ---")
    if res.status_code == 200:
        root = ET.fromstring(res.text)
        for item in root.findall('item')[:3]:
            name_val = item.find('name').get('value') if item.find('name') is not None else "Unknown"
            print(f"ID: {item.get('id')} - Name: {name_val}")
    else:
        print(f"Error {res.status_code}: {res.text[:100]}")
