-- D&D Beyond Catalog Analytics: The Official Supply Signal
-- Dataset: gold_data.analytics_ddb_catalog
-- WotC's own digital catalog: what's available, featured, and priced
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_ddb_catalog`
--        WHERE is_featured = TRUE ORDER BY price DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_ddb_catalog` AS

WITH -- Latest catalog snapshot per SKU
latest AS (
  SELECT sku, title, price, bundle_price, description, product_id, format, is_featured, fetch_date,
    ROW_NUMBER() OVER (PARTITION BY sku ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.dndbeyond_catalog`
),

-- Previous snapshot (for detecting featured rotation and price changes)
prev_snapshot AS (
  SELECT sku, is_featured as was_featured, price as prev_price, fetch_date as prev_date,
    ROW_NUMBER() OVER (PARTITION BY sku ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.dndbeyond_catalog`
  WHERE fetch_date < (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.dndbeyond_catalog`)
),

-- Catalog-wide stats
catalog_stats AS (
  SELECT
    COUNT(DISTINCT sku) as total_skus,
    COUNTIF(is_featured = TRUE) as featured_count,
    AVG(price) as avg_price,
    AVG(bundle_price) as avg_bundle_price
  FROM latest WHERE rn = 1
)

SELECT
  l.sku,
  l.title as concept_name,
  l.format,
  l.price,
  l.bundle_price,
  l.is_featured,
  l.product_id,

  -- Price analysis
  CASE
    WHEN l.price >= 50 THEN 'premium'
    WHEN l.price >= 25 THEN 'standard'
    WHEN l.price >= 10 THEN 'budget'
    ELSE 'free_or_cheap'
  END as price_tier,

  -- Bundle savings (when both prices exist)
  CASE
    WHEN l.bundle_price IS NOT NULL AND l.bundle_price > 0 AND l.price > 0
      THEN ROUND((1.0 - SAFE_DIVIDE(l.bundle_price, l.price)) * 100, 1)
    ELSE NULL
  END as bundle_discount_pct,

  -- Featured rotation tracking
  COALESCE(p.was_featured, FALSE) as was_featured_previously,
  CASE
    WHEN l.is_featured = TRUE AND COALESCE(p.was_featured, FALSE) = FALSE THEN 'newly_featured'
    WHEN l.is_featured = FALSE AND COALESCE(p.was_featured, FALSE) = TRUE THEN 'defeatured'
    WHEN l.is_featured = TRUE THEN 'still_featured'
    ELSE 'not_featured'
  END as featured_status,

  -- Price change detection
  CASE
    WHEN p.prev_price IS NOT NULL AND l.price != p.prev_price
      THEN ROUND(l.price - p.prev_price, 2)
    ELSE NULL
  END as price_change,

  -- Catalog context
  cs.total_skus as catalog_size,
  cs.featured_count,

  -- Standardized output contract
  'supply' as signal_type,
  CASE WHEN l.is_featured THEN 1.0 ELSE 0.0 END as primary_metric,
  CAST(NULL AS FLOAT64) as momentum,  -- supply doesn't have momentum
  'HIGH' as confidence,
  'ddb_catalog' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM latest l
CROSS JOIN catalog_stats cs
LEFT JOIN (SELECT * FROM prev_snapshot WHERE rn = 1) p ON l.sku = p.sku
WHERE l.rn = 1;
