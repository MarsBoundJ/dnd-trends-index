-- DMs Guild & DriveThruRPG Analytics: The Creator Marketplace Signal
-- Dataset: gold_data.analytics_dmsguild_dtrpg
-- Medal-tier product analysis across both OBS marketplaces
--
-- DMs Guild = D&D-exclusive content (WotC license), DTRPG = broader TTRPG market.
-- Comparing the two reveals what's D&D-specific vs industry-wide.
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_dmsguild_dtrpg`
--        WHERE source = 'DMs Guild' AND tier_rank <= 2 ORDER BY price DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_dmsguild_dtrpg` AS

WITH -- Tier hierarchy (lower = more prestigious)
tier_order AS (
  SELECT 'Platinum' as tier, 1 as tier_rank UNION ALL
  SELECT 'Mithral',  2 UNION ALL
  SELECT 'Adamantine', 3 UNION ALL
  SELECT 'Gold', 4 UNION ALL
  SELECT 'Silver', 5 UNION ALL
  SELECT 'General', 6 UNION ALL
  SELECT 'Normal', 7
),

-- Anchor: latest collection date per source
anchors AS (
  SELECT source, MAX(collected_date) as max_date
  FROM `dnd-trends-index.dnd_trends_raw.catalog_supply`
  WHERE source IN ('DMs Guild', 'DriveThruRPG')
  GROUP BY source
),

-- Latest snapshot per title+source, deduped: keep highest tier if duplicates
latest_dedup AS (
  SELECT c.source, c.title, c.seller_tier, c.price, c.rating, c.tags,
    c.collected_date,
    ROW_NUMBER() OVER (
      PARTITION BY c.source, c.title
      ORDER BY t.tier_rank ASC, c.collected_date DESC
    ) as rn
  FROM `dnd-trends-index.dnd_trends_raw.catalog_supply` c
  JOIN anchors a ON c.source = a.source AND c.collected_date = a.max_date
  JOIN tier_order t ON c.seller_tier = t.tier
),
latest AS (
  SELECT * FROM latest_dedup WHERE rn = 1
),

-- Previous snapshot per title+source (for tier movement detection)
prev_anchors AS (
  SELECT c.source, MAX(c.collected_date) as prev_date
  FROM `dnd-trends-index.dnd_trends_raw.catalog_supply` c
  JOIN anchors a ON c.source = a.source
  WHERE c.collected_date < a.max_date
    AND c.source IN ('DMs Guild', 'DriveThruRPG')
  GROUP BY c.source
),
prev_dedup AS (
  SELECT c.source, c.title, c.seller_tier as prev_tier,
    ROW_NUMBER() OVER (
      PARTITION BY c.source, c.title
      ORDER BY t.tier_rank ASC
    ) as rn
  FROM `dnd-trends-index.dnd_trends_raw.catalog_supply` c
  JOIN prev_anchors pa ON c.source = pa.source AND c.collected_date = pa.prev_date
  JOIN tier_order t ON c.seller_tier = t.tier
),
previous AS (
  SELECT source, title, prev_tier FROM prev_dedup WHERE rn = 1
),

-- Catalog-wide stats per source
catalog_stats AS (
  SELECT
    source,
    COUNT(DISTINCT title) as total_products,
    COUNT(DISTINCT seller_tier) as tier_count,
    AVG(price) as avg_price,
    COUNTIF(price = 0) as free_count,
    COUNTIF(price > 0) as paid_count
  FROM latest
  GROUP BY source
),

-- Tier distribution per source
tier_dist AS (
  SELECT source, seller_tier, COUNT(*) as cnt
  FROM latest
  GROUP BY source, seller_tier
)

SELECT
  l.title as concept_name,
  l.source,
  l.seller_tier,
  t.tier_rank,

  -- Pricing
  l.price,
  CASE
    WHEN l.price = 0 THEN 'free'
    WHEN l.price < 5 THEN 'budget'
    WHEN l.price < 15 THEN 'standard'
    WHEN l.price < 30 THEN 'premium'
    ELSE 'deluxe'
  END as price_tier,

  -- Tier movement (promotion = moved to higher tier, demotion = dropped)
  p.prev_tier,
  CASE
    WHEN p.prev_tier IS NULL THEN 'new_or_stable'
    WHEN pt.tier_rank < t.tier_rank THEN 'demoted'
    WHEN pt.tier_rank > t.tier_rank THEN 'promoted'
    ELSE 'stable'
  END as tier_movement,

  -- Catalog context
  cs.total_products as catalog_size,
  cs.avg_price as catalog_avg_price,
  ROUND(SAFE_DIVIDE(cs.free_count, GREATEST(cs.total_products, 1)), 3) as free_ratio,

  -- Tier density: what fraction of the catalog is in this tier
  ROUND(SAFE_DIVIDE(
    td.cnt,
    GREATEST(cs.total_products, 1)
  ), 3) as tier_density,

  -- D&D exclusivity signal: DMs Guild = 100% D&D, DTRPG = mixed TTRPG
  CASE
    WHEN l.source = 'DMs Guild' THEN TRUE
    ELSE FALSE
  END as is_dnd_exclusive,

  -- Standardized output contract
  'creator_marketplace' as signal_type,
  -- Primary metric: inverse tier rank normalized 0-1 (Platinum=1.0, Normal=0.14)
  ROUND(1.0 - SAFE_DIVIDE(CAST(t.tier_rank - 1 AS FLOAT64), 6.0), 3) as primary_metric,
  -- Momentum from tier movement
  CASE
    WHEN p.prev_tier IS NULL THEN CAST(NULL AS FLOAT64)
    WHEN pt.tier_rank < t.tier_rank THEN -0.5  -- demoted
    WHEN pt.tier_rank > t.tier_rank THEN 0.5   -- promoted
    ELSE 0.0
  END as momentum,
  CASE
    WHEN t.tier_rank <= 2 THEN 'HIGH'    -- Platinum/Mithral = proven bestsellers
    WHEN t.tier_rank <= 4 THEN 'MEDIUM'  -- Adamantine/Gold = solid sellers
    ELSE 'LOW'                           -- Silver and below
  END as confidence,
  CASE
    WHEN l.source = 'DMs Guild' THEN 'dmsguild'
    ELSE 'dtrpg'
  END as stream_name,
  CURRENT_DATE() as snapshot_date

FROM latest l
JOIN tier_order t ON l.seller_tier = t.tier
JOIN catalog_stats cs ON l.source = cs.source
JOIN tier_dist td ON l.source = td.source AND l.seller_tier = td.seller_tier
LEFT JOIN previous p ON l.source = p.source AND l.title = p.title
LEFT JOIN tier_order pt ON p.prev_tier = pt.tier;
