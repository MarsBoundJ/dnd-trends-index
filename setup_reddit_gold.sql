-- Phase 37: Create Reddit Social Leaderboards View
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_social_leaderboards` AS
WITH daily_agg AS (
    SELECT 
        matched_concept as name,
        matched_category as category,
        extraction_date as date,
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) as mentions
    FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
    GROUP BY 1, 2, 3
),
history_trails AS (
    SELECT 
        name,
        category,
        ARRAY_AGG(
            CASE 
                WHEN avg_sentiment > 0.1 THEN 1
                WHEN avg_sentiment < -0.1 THEN -1
                ELSE 0
            END ORDER BY date DESC LIMIT 7
        ) as history
    FROM daily_agg
    GROUP BY 1, 2
),
latest_stats AS (
    SELECT 
        name,
        category,
        avg_sentiment as sentiment,
        mentions as score,
        RANK() OVER(PARTITION BY category ORDER BY mentions DESC) as rank_position
    FROM daily_agg
    WHERE date = (SELECT MAX(date) FROM daily_agg)
),
sector_stats AS (
    SELECT 
        category, 
        AVG(score) as heat_score
    FROM latest_stats
    WHERE rank_position <= 10
    GROUP BY 1
)
SELECT 
    l.category,
    l.name,
    l.score as metric_score,
    l.sentiment,
    h.history,
    'reddit' as source,
    l.rank_position,
    s.heat_score
FROM latest_stats l
JOIN history_trails h ON l.name = h.name AND l.category = h.category
JOIN sector_stats s ON l.category = s.category;
