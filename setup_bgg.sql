-- Phase 40: BGG Ingestion Setup
CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_categorized.bgg_id_map` (
    concept_name STRING,
    bgg_id INT64,
    last_verified DATE
);

CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.bgg_product_stats` (
    date DATE,
    concept_name STRING,
    owned_count INT64,
    quality_score FLOAT64
);

-- Seed Initial Mappings
INSERT INTO `dnd-trends-index.dnd_trends_categorized.bgg_id_map` (concept_name, bgg_id, last_verified)
VALUES 
    ('Player\'s Handbook (2024)', 411830, CURRENT_DATE()),
    ('Monster Manual', 15655, CURRENT_DATE()),
    ('Dungeon Master\'s Guide', 16188, CURRENT_DATE()),
    ('Curse of Strahd', 193301, CURRENT_DATE()),
    ('Eberron: Rising from the Last War', 288647, CURRENT_DATE());
