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
ORDER BY s.heat_score DESC, r.score DESC