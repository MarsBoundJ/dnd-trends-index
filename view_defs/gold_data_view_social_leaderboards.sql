WITH daily_agg AS (
    SELECT 
        keyword as name,
        extraction_date as date,
        AVG(weighted_score) as sentiment, 
        SUM(mention_count) as mentions
    FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
    GROUP BY 1, 2
),
history_trails AS (
    SELECT 
        name,
        ARRAY_AGG(
            CASE 
                WHEN mentions > 10 THEN 1 -- High volume spike
                WHEN mentions < 2 THEN -1 -- Low volume drop
                ELSE 0
            END ORDER BY date DESC LIMIT 7
        ) as history
    FROM daily_agg
    GROUP BY 1
),
latest_stats AS (
    SELECT 
        name,
        sentiment,
        mentions as score
    FROM daily_agg
    WHERE date = (SELECT MAX(date) FROM daily_agg)
),
joined_data AS (
    SELECT 
        COALESCE(c.category, 'Uncategorized') as category,
        l.name,
        l.score as metric_score,
        l.sentiment,
        COALESCE(h.history, []) as history,
        'reddit' as source
    FROM latest_stats l
    LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON l.name = c.concept_name
    LEFT JOIN history_trails h ON l.name = h.name
),
sector_stats AS (
    SELECT 
        category, 
        AVG(metric_score) as heat_score
    FROM joined_data
    GROUP BY 1
)
SELECT 
    j.category,
    j.name,
    j.metric_score,
    j.sentiment,
    j.history,
    j.source,
    RANK() OVER(PARTITION BY j.category ORDER BY j.metric_score DESC) as rank_position,
    s.heat_score
FROM joined_data j
JOIN sector_stats s ON j.category = s.category