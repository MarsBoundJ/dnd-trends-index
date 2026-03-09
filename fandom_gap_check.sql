SELECT 
    article_title, 
    hype_score,
    TRIM(REGEXP_REPLACE(article_title, r' \(.*\)$', '')) as normalized_key
FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics` 
WHERE TRIM(REGEXP_REPLACE(article_title, r' \(.*\)$', '')) NOT IN (SELECT concept_name FROM `dnd-trends-index.dnd_trends_categorized.concept_library`) 
ORDER BY hype_score DESC 
LIMIT 10;
