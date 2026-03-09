WITH latest_stats AS (
    SELECT 
        s.concept_name,
        s.owned_count,
        s.quality_score,
        n.score_buy,
        s.date
    FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats` s
    JOIN `dnd-trends-index.silver_data.norm_buy` n 
      ON s.concept_name = n.keyword AND s.date = n.date
    WHERE s.date = (SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`)
)
SELECT 
    COALESCE(c.category, 'Rulebooks') as category,
    l.concept_name,
    l.owned_count,
    l.quality_score,
    l.score_buy * 100 as metric_score,
    'bgg' as source,
    RANK() OVER(PARTITION BY COALESCE(c.category, 'Rulebooks') ORDER BY l.owned_count DESC) as rank_position,
    l.score_buy * 100 as heat_score
FROM latest_stats l
LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON l.concept_name = c.concept_name