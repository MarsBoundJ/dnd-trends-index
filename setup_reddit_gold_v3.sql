-- Phase 37: Create Reddit Social Leaderboards View (FIXED V2)
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_social_leaderboards` AS
WITH daily_agg AS (
    SELECT 
        keyword as name,
        category,
        extraction_date as date,
        -- Use weighted_score as a proxy for 'heat' if sentiment_score is missing
        AVG(weighted_score) as sentiment, 
        SUM(mention_count) as mentions
    FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
    GROUP BY 1, 2, 3
),
history_trails AS (
    SELECT 
        name,
        category,
        ARRAY_AGG(
            CASE 
                WHEN mentions > 10 THEN 1 -- High volume spike
                WHEN mentions < 2 THEN -1 -- Low volume drop
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
        sentiment,
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
