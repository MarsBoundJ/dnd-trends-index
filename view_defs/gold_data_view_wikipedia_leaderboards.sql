WITH recent_data AS (
        SELECT 
            TRIM(REGEXP_REPLACE(keyword, r' \(.*\)$', '')) as clean_wiki_key,
            views,
            COALESCE(score_wiki * 100, 0) as score,
            ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY date DESC) as rn,
            date
        FROM `silver_data.norm_wikipedia`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    ),
    latest_data AS (
        SELECT * FROM recent_data WHERE rn = 1
    ),
    categorized AS (
        SELECT 
            c.category,
            c.concept_name as canonical_concept,
            COALESCE(AVG(l.score), 0) as average_score,
            COALESCE(SUM(l.views), 0) as total_views,
            MAX(l.date) as last_updated
        FROM `dnd_trends_categorized.concept_library` c
        LEFT JOIN latest_data l ON LOWER(c.concept_name) = LOWER(l.clean_wiki_key)
        WHERE c.is_active = TRUE
        GROUP BY c.category, c.concept_name
    ),
    sector_scores AS (
        SELECT 
            category,
            canonical_concept,
            total_views as views,
            average_score as score,
            AVG(average_score) OVER (PARTITION BY category) as heat_score
        FROM categorized
    )
    SELECT * FROM sector_scores ORDER BY heat_score DESC, score DESC