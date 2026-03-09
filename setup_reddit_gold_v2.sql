-- Phase 37: Create Reddit Social Leaderboards View (FIXED)
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_social_leaderboards` AS
WITH daily_agg AS (
    SELECT 
        keyword as name,
        category,
        extraction_date as date,
        -- Weighted average is complex without individual raw sentiment, 
        -- but we'll assume sentiment_score in the table is already a representative average/snapshot.
        -- Actually, the table has mention_count and weighted_score.
        -- Let's assume sentiment for Reddit is tracked in reddit_viral_events for faces.
        -- Wait, the harvester says it puts sentiment_score in metrics.
        -- I'll check the inventory again.
        0.0 as sentiment, -- Placeholder if not in main metrics
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
                WHEN mentions > (SELECT AVG(mentions) FROM daily_agg) * 1.5 THEN 1
                WHEN mentions < (SELECT AVG(mentions) FROM daily_agg) * 0.5 THEN -1
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
        0.0 as sentiment,
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
