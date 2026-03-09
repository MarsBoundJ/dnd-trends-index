CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_api_leaderboards` AS
WITH ranked AS (
    SELECT 
        category,
        canonical_concept,
        google_score_avg,
        RANK() OVER (PARTITION BY category ORDER BY google_score_avg DESC) as rank_position
    FROM `dnd-trends-index.gold_data.deep_dive_metrics`
    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.gold_data.deep_dive_metrics`)
),
sector_stats AS (
    SELECT 
        category, 
        AVG(google_score_avg) as heat_score -- The "Sector Heat"
    FROM ranked 
    WHERE rank_position <= 20 -- Base heat on the Top 20 performers
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
-- CRITICAL: Order by Heat first, then Rank
ORDER BY s.heat_score DESC, r.rank_position ASC;
