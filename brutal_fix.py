from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

def brutal_fix(view_id):
    try:
        table = client.get_table(view_id)
        query = table.view_query
        
        # BRUTE FORCE string replacements to catch all variants
        replacements = {
            '`dnd-trends-index.social_data.youtube_videos`': '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`',
            'dnd-trends-index.social_data.youtube_videos': '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`',
            '`social_data.youtube_videos`': '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`',
            'social_data.youtube_videos': '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`',
            '`youtube_videos`': '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`',
             'youtube_videos': '`dnd-trends-index.dnd_trends_raw.yt_video_intelligence`'
        }
        
        new_query = query
        for old, new in replacements.items():
            new_query = new_query.replace(old, new)
            
        if new_query != query:
            sql = f"CREATE OR REPLACE VIEW `{view_id}` AS\n{new_query}"
            client.query(sql).result()
            print(f"BRUTAL SUCCESS: {view_id} updated.")
        else:
            print(f"NO CHANGE NEEDED OR FAILED TO MATCH: {view_id}")
            
    except Exception as e:
        print(f"ERROR: {e}")

brutal_fix('dnd-trends-index.silver_data.norm_youtube')
brutal_fix('dnd-trends-index.gold_data.view_sentiment_divergence')
