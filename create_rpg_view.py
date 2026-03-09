from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

# Get BGG View Query
table_bgg = client.get_table('dnd-trends-index.gold_data.view_bgg_leaderboards')
bgg_sql = table_bgg.view_query

# Create RPGGeek View Query by replacing Table ID
rpg_sql = bgg_sql.replace('bgg_product_stats', 'rpggeek_product_stats')
rpg_sql = rpg_sql.replace("'BoardGameGeek'", "'RPGGeek'")

print("Original BGG SQL Snippet:", bgg_sql[:100])
print("New RPG SQL Snippet:", rpg_sql[:100])

# Define the new view
view_id = "dnd-trends-index.gold_data.view_rpggeek_leaderboards"
view = bigquery.Table(view_id)
view.view_query = rpg_sql

# Create or Update the view
try:
    client.delete_table(view_id, not_found_ok=True)
    client.create_table(view)
    print(f"Successfully created view: {view_id}")
except Exception as e:
    print(f"Error creating view: {e}")
