SELECT
        video_id,
        title,
        channel_name,
        velocity_24h,
        sentiment_pos_ratio
    FROM `dnd-trends-index.social_data.youtube_videos`
    WHERE velocity_24h > 1000
      AND sentiment_pos_ratio < 0.3
    ORDER BY velocity_24h DESC
