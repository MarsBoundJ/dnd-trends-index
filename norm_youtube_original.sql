WITH daily_raw AS (
        SELECT 
            DATE(published_at) as date,
            kw as keyword,
            MAX(velocity_24h) as max_velocity
        FROM `dnd-trends-index.social_data.youtube_videos`,
        UNNEST(matched_keywords) as kw
        GROUP BY 1, 2
    )
    SELECT 
        date,
        keyword,
        PERCENT_RANK() OVER(PARTITION BY date ORDER BY max_velocity ASC) as score_youtube
    FROM daily_raw
