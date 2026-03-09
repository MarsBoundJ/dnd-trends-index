Á
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"

def create_views():
    client = bigquery.Client(project=PROJECT_ID)
    
    # 1. view_trend_spikes
    # Logic: Score > 20 AND Growth > 50% in 7 days
    sql_spikes = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.view_trend_spikes` AS
    SELECT 
        t1.keyword,
        t1.trend_score_raw as current_score,
        t2.trend_score_raw as previous_score,
        ((t1.trend_score_raw - t2.trend_score_raw) / NULLIF(t2.trend_score_raw, 0)) as growth_pct
    FROM `{PROJECT_ID}.{DATASET_ID}.trend_scores` t1
    JOIN `{PROJECT_ID}.{DATASET_ID}.trend_scores` t2
        ON t1.keyword = t2.keyword
    WHERE t1.date = CURRENT_DATE()
      AND t2.date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND t1.trend_score_raw > 20
      AND ((t1.trend_score_raw - t2.trend_score_raw) / NULLIF(t2.trend_score_raw, 0)) > 0.5
    ORDER BY growth_pct DESC
    """
    
    # 2. view_platform_gaps
    # Logic: High Hype (>0.8) vs Low Play (<0.4) OR High Play (>0.8) vs Low Hype (<0.4)
    sql_gaps = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.view_platform_gaps` AS
    SELECT
        keyword,
        hype_score,
        play_score,
        (hype_score - play_score) as gap,
        CASE
            WHEN hype_score > 0.8 AND play_score < 0.4 THEN 'Ghost Hype'
            WHEN play_score > 0.8 AND hype_score < 0.4 THEN 'Silent Killer'
            ELSE 'Other'
        END as narrative_type
    FROM `{PROJECT_ID}.{DATASET_ID}.trend_scores`
    WHERE date = CURRENT_DATE()
      AND (
          (hype_score > 0.8 AND play_score < 0.4) OR 
          (play_score > 0.8 AND hype_score < 0.4)
      )
    ORDER BY ABS(gap) DESC
    """
    
    # 3. view_sentiment_divergence
    # Logic: Viral Videos (>1000 views/24h) with Negative Sentiment (<0.3 positivity)
    # Using YouTube as proxy for now since Reddit data isn't fully keyed yet.
    sql_sentiment = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.view_sentiment_divergence` AS
    SELECT
        video_id,
        title,
        channel_name,
        velocity_24h,
        sentiment_pos_ratio
    FROM `{PROJECT_ID}.social_data.youtube_videos`
    WHERE velocity_24h > 1000
      AND sentiment_pos_ratio < 0.3
    ORDER BY velocity_24h DESC
    """
    
    views = [
        ("view_trend_spikes", sql_spikes),
        ("view_platform_gaps", sql_gaps),
        ("view_sentiment_divergence", sql_sentiment)
    ]
    
    print("Creating Narrative Views...")
    for name, sql in views:
        try:
            client.query(sql).result()
            print(f"âœ… Created {name}")
        except Exception as e:
            print(f"âŒ Error creating {name}: {e}")

if __name__ == "__main__":
    create_views()
Á"(0346c7b262db785b9f82b154e34994382565350e27file:///C:/Users/Yorri/.gemini/setup_narrative_views.py:file:///C:/Users/Yorri/.gemini