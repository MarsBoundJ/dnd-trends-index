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
WHERE c.is_active = TRUE