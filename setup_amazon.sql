-- Phase 42: Amazon Market Watch Setup
CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_categorized.amazon_asin_map` (
    concept_name STRING,
    asin STRING, -- Amazon ID
    product_type STRING
);

CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.amazon_daily_stats` (
    asin STRING,
    rank INT64,
    price_cents INT64,
    date DATE
);

-- Seed Initial ASINs
INSERT INTO `dnd-trends-index.dnd_trends_categorized.amazon_asin_map` (concept_name, asin, product_type)
VALUES 
    ('Player\'s Handbook (2024)', '0786969514', 'Rulebook'),
    ('Monster Manual', '0786965616', 'Rulebook'),
    ('The Monsters Know What They\'re Doing', '1982122668', 'Supplement'),
    ('Dungeon Master\'s Guide', '0786965624', 'Rulebook'),
    ('Curse of Strahd', '0786965985', 'Adventure');
