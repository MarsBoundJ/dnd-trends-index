import rpggeek_client
import time

targets = [
    "Dungeons & Dragons Player's Handbook (2024)",
    "Shadowdark RPG",
    "Daggerheart",
    "Draw Steel"
]

results = {}
for target in targets:
    print(f"Searching for {target}...")
    hits = rpggeek_client.search_rpg_items(target)
    if hits:
        # Just grab the first hit and print it
        results[target] = hits[0]
    time.sleep(2)

print("\n--- RESULTS ---")
for t, hit in results.items():
    print(f"{t} -> ID: {hit['id']} (Name: {hit['name']})")
