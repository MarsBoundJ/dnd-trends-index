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
        if 'youtube_videos' in query:
            print(f"!!! FOUND broken reference in {v} !!!")
        else:
            print(f"OK: {v}")
            if 'yt_video_intelligence' in query:
                print(f"  (Contains yt_video_intelligence)")
    except Exception as e:
        print(f"Error checking {v}: {e}")
