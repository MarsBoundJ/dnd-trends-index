-- Normalization: Convert Ownership count to 0.0-1.0 score
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_buy` AS
SELECT 
    date,
    concept_name as keyword,
    PERCENT_RANK() OVER (PARTITION BY date ORDER BY owned_count ASC) as score_buy
FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`;
