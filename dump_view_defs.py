from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

views = [
    'gold_data.view_api_leaderboards',
    'gold_data.view_fandom_leaderboards',
    'gold_data.view_wikipedia_leaderboards',
    'gold_data.view_social_leaderboards'
]

for v in views:
    try:
        table = client.get_table(f'dnd-trends-index.{v}')
        query = table.view_query
        print(f"--- {v} ---")
        print(query)
        print("-" * 40)
    except Exception as e:
        print(f"Error checking {v}: {e}")
