from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

def fix_view(view_id):
    table = client.get_table(view_id)
    query = table.view_query

    print(f"--- ORIGINAL {view_id} ---")
    print(query)

    # Simple replace
    new_query = query.replace('social_data.youtube_videos', 'dnd_trends_raw.yt_video_intelligence')
    new_query = new_query.replace('`youtube_videos`', '`dnd_trends_raw.yt_video_intelligence`')
    
    # If the view was created with unqualified names, try harder
    if 'dnd_trends_raw.yt_video_intelligence' not in new_query:
        new_query = query.replace('youtube_videos', '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`')

    print(f"--- NEW {view_id} ---")
    print(new_query)
    
    try:
        # Recreate the view
        sql = f"CREATE OR REPLACE VIEW `{view_id}` AS\n{new_query}"
        client.query(sql).result()
        print(f"SUCCESS: {view_id} fixed.")
    except Exception as e:
        print(f"ERROR fixing {view_id}: {e}")

fix_view('dnd-trends-index.silver_data.norm_youtube')
fix_view('dnd-trends-index.gold_data.view_sentiment_divergence')
