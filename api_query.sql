WITH ranked AS (
    SELECT 
        m.category,
        m.canonical_name as canonical_concept,
        m.google_score_avg,
        ROW_NUMBER() OVER (PARTITION BY m.category ORDER BY m.google_score_avg DESC) as rank_position
    FROM `dnd-trends-index.gold_data.deep_dive_metrics` m
    INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON m.canonical_name = c.concept_name
    WHERE c.is_active = TRUE
),
sector_stats AS (
    SELECT 
        category, 
        AVG(google_score_avg) as heat_score
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
ORDER BY s.heat_score DESC, r.rank_position ASC
