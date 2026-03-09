CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_wikipedia_leaderboards` AS
WITH latest_data AS (
    SELECT 
        keyword as canonical_concept,
        -- Ensure we use the latest available date
        views,
        -- Scale Silver score (0.0-1.0) to UI score (0-100)
        COALESCE(score_wiki * 100, 0) as score
    FROM `dnd-trends-index.silver_data.norm_wikipedia`
    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.silver_data.norm_wikipedia`)
),
categorized AS (
    SELECT 
        l.canonical_concept,
        l.score,
        c.category
    FROM latest_data l
    INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c
        ON LOWER(l.canonical_concept) = LOWER(c.concept_name)
    WHERE c.is_active = TRUE
),
sector_stats AS (
    SELECT category, AVG(score) as heat_score
    FROM categorized
    GROUP BY 1
)
SELECT 
    c.category,
    c.canonical_concept,
    c.score,
    s.heat_score,
    'wikipedia' as source, -- Hardcode source for Badge Logic
    RANK() OVER (PARTITION BY c.category ORDER BY c.score DESC) as rank_position
FROM categorized c
JOIN sector_stats s ON c.category = s.category
ORDER BY s.heat_score DESC, c.score DESC;
