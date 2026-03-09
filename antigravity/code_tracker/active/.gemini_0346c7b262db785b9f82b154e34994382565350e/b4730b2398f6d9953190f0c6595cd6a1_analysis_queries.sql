–
-- Query 1: The "Meteoric Riser"
-- Finds keywords with >50% growth in Trend Score over the last 7 days.
-- Returns: keyword, growth_pct, current_score, previous_score
SELECT
    t1.keyword,
    ((t1.trend_score_raw - t2.trend_score_raw) / NULLIF(t2.trend_score_raw, 0)) as growth_pct,
    t1.trend_score_raw as current_score,
    t2.trend_score_raw as previous_score
FROM `{PROJECT_ID}.gold_data.trend_scores` t1
JOIN `{PROJECT_ID}.gold_data.trend_scores` t2
    ON t1.keyword = t2.keyword
WHERE t1.date = CURRENT_DATE()
  AND t2.date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND t1.trend_score_raw > 20 -- Filter out low-level noise
  AND ((t1.trend_score_raw - t2.trend_score_raw) / NULLIF(t2.trend_score_raw, 0)) > 0.5
ORDER BY growth_pct DESC
LIMIT 5;

-- Query 2: The "Platform Gap" (Ghost Hype vs Silent Killer)
-- Compares Hype (Talk) vs Play (Action).
-- Ghost Hype: High Hype, Low Play (Talking but not Playing)
-- Silent Killer: High Play, Low Hype (Playing but not Talking)
SELECT
    keyword,
    hype_score,
    play_score,
    (hype_score - play_score) as gap,
    CASE
        WHEN (hype_score - play_score) > 0.4 THEN 'Ghost Hype'
        WHEN (play_score - hype_score) > 0.4 THEN 'Silent Killer'
        ELSE 'Balanced'
    END as narrative_type
FROM `{PROJECT_ID}.gold_data.trend_scores`
WHERE date = CURRENT_DATE()
  AND ABS(hype_score - play_score) > 0.4
ORDER BY ABS(gap) DESC
LIMIT 5;

-- Query 3: The "Sentiment Extremes" (YouTube)
-- Finds videos with high velocity and extreme sentiment.
SELECT
    video_id,
    title,
    channel_name,
    view_count,
    velocity_24h,
    sentiment_pos_ratio
FROM `{PROJECT_ID}.social_data.youtube_videos`
WHERE velocity_24h > 1000 -- Viral threshold
ORDER BY sentiment_pos_ratio DESC
LIMIT 5;
–"(0346c7b262db785b9f82b154e34994382565350e23file:///C:/Users/Yorri/.gemini/analysis_queries.sql:file:///C:/Users/Yorri/.gemini