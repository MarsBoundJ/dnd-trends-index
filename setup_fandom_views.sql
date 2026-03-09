-- Task 1: Create Fandom Mapping View
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.view_fandom_mapping` AS
WITH clean_fandom AS (
    SELECT 
        wiki_slug,
        article_title as raw_title,
        -- Aggressive Cleaning: Remove any text in parenthesis at the end
        -- e.g. "Fireball (Spell)" -> "Fireball"
        TRIM(REGEXP_REPLACE(article_title, r' \(.*\)$', '')) as clean_key,
        hype_score,
        extraction_date
    FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
)
SELECT 
    f.wiki_slug,
    f.raw_title,
    f.hype_score,
    f.extraction_date,
    c.concept_name as canonical_concept,
    c.category
FROM clean_fandom f
INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c
    ON LOWER(f.clean_key) = LOWER(c.concept_name)
WHERE c.is_active = TRUE;

-- Task 2: Create Fandom Leaderboard View
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_fandom_leaderboards` AS
WITH latest_extraction AS (
    SELECT MAX(extraction_date) as max_date FROM `dnd-trends-index.silver_data.view_fandom_mapping`
),
ranked_items AS (
    SELECT 
        category,
        canonical_concept,
        -- Scale 0.0-1.0 score to 0-100
        MAX(hype_score) * 100 as score,
        -- Identify which wiki is driving the trend
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
