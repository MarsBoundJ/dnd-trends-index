-- Gold Layer: Amazon Leaderboard View
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_amazon_leaderboards` AS
WITH latest_stats AS (
    SELECT 
        s.asin,
        s.rank,
        s.price_cents,
        s.date,
        m.concept_name
    FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats` s
    JOIN `dnd-trends-index.dnd_trends_categorized.amazon_asin_map` m ON s.asin = m.asin
    WHERE s.date = (SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`)
)
SELECT 
    COALESCE(c.category, 'Rulebooks') as category,
    l.concept_name,
    l.asin as source_id,
    l.rank as sales_rank,
    l.price_cents / 100.0 as price,
    n.score_buy * 100 as metric_score,
    'amazon' as source,
    RANK() OVER(PARTITION BY COALESCE(c.category, 'Rulebooks') ORDER BY l.rank ASC) as rank_position,
    (1.0 - PERCENT_RANK() OVER(PARTITION BY COALESCE(c.category, 'Rulebooks') ORDER BY l.rank ASC)) * 100 as heat_score
FROM latest_stats l
LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON l.concept_name = c.concept_name
LEFT JOIN `dnd-trends-index.silver_data.norm_buy` n ON l.concept_name = n.keyword AND l.date = n.date;
