from google.cloud import bigquery
import re

client = bigquery.Client(project='dnd-trends-index')

def fix_view(view_id):
    table = client.get_table(view_id)
    query = table.view_query

    print(f"--- ORIGINAL {view_id} ---")
    print(query)

    # Use regex to replace any variation of youtube_videos with the correct fully qualified name
    # Matches: social_data.youtube_videos, `social_data.youtube_videos`, `dnd-trends-index.social_data.youtube_videos`, etc.
    new_query = re.sub(
        r'`?(?:dnd-trends-index\.)?social_data\.youtube_videos`?',
        '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`',
        query
    )

    print(f"--- NEW {view_id} ---")
    print(new_query)
    
    try:
        sql = f"CREATE OR REPLACE VIEW `{view_id}` AS\n{new_query}"
        client.query(sql).result()
        print(f"SUCCESS: {view_id} fixed.")
    except Exception as e:
        print(f"ERROR fixing {view_id}: {e}")

fix_view('dnd-trends-index.silver_data.norm_youtube')
fix_view('dnd-trends-index.gold_data.view_sentiment_divergence')
