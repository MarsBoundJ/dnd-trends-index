-- Task 1: Taxonomy Fix
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library` 
SET category = 'Mechanic' 
WHERE category IN ('combat', 'core_mechanics', 'character_creation', 'Game Mechanic');

UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library` 
SET category = 'Equipment' 
WHERE category = 'equipment';

UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library` 
SET category = 'UA Content' 
WHERE category = 'Unearthed Arcana';

UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library` 
SET category = 'Classification' 
WHERE category = 'Classification';

-- Task 2: Refresh Gold Views
-- Google Trends View (with Dense Snapshot logic preserved)
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_api_leaderboards` AS
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
ORDER BY s.heat_score DESC, r.rank_position ASC;

-- Fandom Leaderboard View
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_fandom_leaderboards` AS
WITH latest_extraction AS (
    SELECT MAX(extraction_date) as max_date FROM `dnd-trends-index.silver_data.view_fandom_mapping`
),
ranked_items AS (
    SELECT 
        category,
        canonical_concept,
        MAX(hype_score) * 100 as score,
        ARRAY_AGG(wiki_slug ORDER BY hype_score DESC LIMIT 1)[OFFSET(0)] as top_wiki
    FROM `dnd-trends-index.silver_data.view_fandom_mapping`
    WHERE extraction_date = (SELECT max_date FROM latest_extraction)
    GROUP BY 1, 2
),
sector_stats AS (
    SELECT category, AVG(score) as heat_score
    FROM ranked_items 
    GROUP BY 1
)
SELECT 
    r.category,
    r.canonical_concept,
    r.score,
    r.top_wiki,
    ROUND(s.heat_score, 1) as heat_score,
    ROW_NUMBER() OVER (PARTITION BY r.category ORDER BY r.score DESC) as rank_position
FROM ranked_items r
JOIN sector_stats s ON r.category = s.category
ORDER BY s.heat_score DESC, r.score DESC;
