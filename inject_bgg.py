from google.cloud import bigquery
import time
import subprocess

client = bigquery.Client(project='dnd-trends-index')

bgg_data = [
    ("Tasha's Cauldron of Everything", 323608),
    ("Xanathar's Guide to Everything", 236203),
    ("Volo's Guide to Monsters", 209355),
    ("Mordenkainen's Tome of Foes", 246083),
    ("Fizban's Treasury of Dragons", 346571),
    ("Spelljammer: Adventures in Space", 367399),
    ("Van Richten's Guide to Ravenloft", 332616),
    ("Guildmasters' Guide to Ravnica", 256191),
    ("Mythic Odysseys of Theros", 304245),
    ("Eberron: Rising from the Last War", 288647),
    ("Curse of Strahd", 193301),
    ("Tomb of Annihilation", 230718),
    ("Waterdeep: Dragon Heist", 251410),
    ("Baldur's Gate: Descent into Avernus", 280145),
    ("Icewind Dale: Rime of the Frostmaiden", 312214),
    ("The Wild Beyond the Witchlight", 341050),
    ("Dragonlance: Shadow of the Dragon Queen", 367401),
    ("Journeys through the Radiant Citadel", 358485),
    ("Vecna: Eve of Ruin", 412211),
    ("Keys from the Golden Vault", 380451),
    ("Player's Handbook (2014)", 161878),
    ("Monster Manual (2014)", 156553),
    ("Dungeon Master's Guide (2014)", 156554),
    ("Player's Handbook (2024)", 411830),
    ("Monster Manual (2025)", 411832),
    ("Dungeon Master's Guide (2024)", 411831)
]

rows = [{"concept_name": b, "bgg_id": i} for b, i in bgg_data]
names = [f"'{b.replace(chr(39), chr(92)+chr(39))}'" for b, i in bgg_data]
del_query = f"DELETE FROM `dnd-trends-index.dnd_trends_categorized.bgg_id_map` WHERE concept_name IN ({','.join(names)})"

print("Injecting BGG IDs into BigQuery...")
client.query(del_query).result()
errors = client.insert_rows_json("dnd-trends-index.dnd_trends_categorized.bgg_id_map", rows)
if errors:
    print(f"Insert errors: {errors}")
else:
    print(f"Successfully inserted {len(rows)} IDs into mapping table.")

print("Triggering BGG Harvester...")
import os
os.system("python /app/harvesters/bgg_harvester.py")
