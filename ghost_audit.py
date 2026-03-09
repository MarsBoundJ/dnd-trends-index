from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
ghosts = [
    'Caleb Widogast', 'Mollymauk Tealeaf', 'Ireena Kolyana', 'Kas', 'Raistlin Majere', 
    'Fizban the Fabulous', 'Lady of Pain', 'Iuz', 'Planescape', 'Abyss', 
    'Mighty Nein', 'Space clown', 'Borys', 'Sorcerer-Kings', 'Natural 20', 'Creature type'
]

print("--- Fandom Ghost Status Audit ---")
sql = f"SELECT concept_name, status FROM `dnd-trends-index.dnd_trends_raw.ai_suggestions` WHERE concept_name IN {tuple(ghosts)}"
results = client.query(sql).result()
status_map = {row.concept_name: row.status for row in results}

for g in ghosts:
    status = status_map.get(g, "NOT FOUND")
    print(f"{g}: {status}")

sql_lib = "SELECT concept_name, is_active FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE concept_name IN ('Iuz', 'Space clown')"
print("\n--- Library Check ---")
for row in client.query(sql_lib).result():
    print(f"{row.concept_name}: is_active = {row.is_active}")
