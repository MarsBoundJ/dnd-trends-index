CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_leaderboard_clean` AS
WITH cleaned AS (
    SELECT
        -- 1. Strip Suffixes for Display (Using original_keyword as canonical if available, else regex)
        COALESCE(e.original_keyword, TRIM(REGEXP_REPLACE(t.search_term, r'(?i) (5e|Dnd|2024|dnd|5\.5|price|build)$', ''))) as display_name,
        COALESCE(e.category, 'Uncategorized') as category,
        t.interest as trend_score_raw
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    LEFT JOIN `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` e ON t.search_term = e.search_term
)
SELECT 
    display_name,
    category,
    -- 3. Max-Signal Aggregation
    MAX(trend_score_raw) as score,
    -- 4. Count variation count
    COUNT(*) as variation_count
FROM cleaned
GROUP BY 1, 2
ORDER BY score DESC;

-- Update API View
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_api_leaderboards` AS
SELECT 
    category,
    display_name as canonical_concept,
    score as google_score_avg,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) as rank_position
FROM `dnd-trends-index.gold_data.view_leaderboard_clean`
WHERE category IS NOT NULL AND category != 'Archive';
