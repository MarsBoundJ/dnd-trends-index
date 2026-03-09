CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_api_leaderboards` AS
WITH latest_dates AS (
    SELECT category, MAX(date) as max_date
    FROM `dnd-trends-index.gold_data.deep_dive_metrics`
    GROUP BY 1
),
ranked AS (
    SELECT 
        m.category,
        m.canonical_concept,
        m.google_score_avg,
        -- Use ROW_NUMBER to ensure exactly 20 items for heat calculation without gaps for ties
        ROW_NUMBER() OVER (PARTITION BY m.category ORDER BY m.google_score_avg DESC) as rank_position
    FROM `dnd-trends-index.gold_data.deep_dive_metrics` m
    INNER JOIN latest_dates ld ON m.category = ld.category AND m.date = ld.max_date
),
sector_stats AS (
    SELECT 
        category, 
        AVG(google_score_avg) as heat_score -- Sector Heat based on top performers
    FROM ranked 
    WHERE rank_position <= 20
    GROUP BY 1
)
SELECT 
    r.category,
    r.canonical_concept,
    r.google_score_avg,
    r.rank_position,
    s.heat_score
FROM ranked r
JOIN sector_stats s ON r.category = s.category
WHERE r.rank_position <= 40
ORDER BY s.heat_score DESC, r.rank_position ASC;
