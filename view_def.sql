WITH date_stats AS (
    SELECT 
        category, 
        date, 
        COUNT(*) as item_count
    FROM `dnd-trends-index.gold_data.deep_dive_metrics`
    GROUP BY 1, 2
),
max_category_volume AS (
    SELECT category, MAX(item_count) as max_vol
    FROM date_stats
    GROUP BY 1
),
stable_latest_dates AS (
    SELECT 
        ds.category, 
        MAX(ds.date) as latest_stable_date
    FROM date_stats ds
    JOIN max_category_volume mv ON ds.category = mv.category
    WHERE ds.item_count >= mv.max_vol * 0.8
    GROUP BY 1
),
ranked AS (
    SELECT 
        m.category,
        m.canonical_concept,
        m.google_score_avg,
        ROW_NUMBER() OVER (PARTITION BY m.category ORDER BY m.google_score_avg DESC) as rank_position
    FROM `dnd-trends-index.gold_data.deep_dive_metrics` m
    INNER JOIN stable_latest_dates sld ON m.category = sld.category AND m.date = sld.latest_stable_date
    INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON m.canonical_concept = c.concept_name
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