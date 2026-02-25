-- Normalization: Combine Amazon and BGG Commercial Data
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_buy` AS
WITH amazon_momentum AS (
    SELECT 
        date, 
        m.concept_name as keyword, 
        (1.0 / NULLIF(rank, 0)) as momentum,
        s.rank,
        s.price_cents / 100.0 as price
    FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats` s
    JOIN `dnd-trends-index.dnd_trends_categorized.amazon_asin_map` m ON s.asin = m.asin
),
bgg_momentum AS (
    SELECT 
        date, 
        concept_name as keyword, 
        CAST(owned_count AS FLOAT64) as momentum
    FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`
),
combined AS (
    SELECT date, keyword, momentum, rank, price FROM amazon_momentum
    UNION ALL
    SELECT date, keyword, momentum, 0 as rank, 0.0 as price FROM bgg_momentum
)
SELECT 
    date, 
    keyword,
    PERCENT_RANK() OVER (PARTITION BY date ORDER BY momentum ASC) as score_buy,
    AVG(price) OVER (PARTITION BY date, keyword) as current_price,
    MIN(rank) OVER (PARTITION BY date, keyword) as rank
FROM combined;
