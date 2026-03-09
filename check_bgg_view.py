from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

# Get BGG View Query
try:
    table = client.get_table('dnd-trends-index.gold_data.view_bgg_leaderboards')
    print("--- BGG VIEW QUERY ---")
    print(table.view_query)
except Exception as e:
    print(f"BGG Error: {e}")

# Check RPGGeek Tables
try:
    tables = [t.table_id for t in client.list_tables('dnd_trends_raw')]
    print("\n--- RPGGEEK TABLES ---")
    print([t for t in tables if 'rpg' in t])
except Exception as e:
    print(f"RPG Error: {e}")
