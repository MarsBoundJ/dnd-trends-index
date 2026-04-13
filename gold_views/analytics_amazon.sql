-- Amazon Analytics: The Mass Market & Gifting Signal
-- Dataset: gold_data.analytics_amazon
-- Per-product sales rank, pricing, and rating analysis
--
-- Note: Amazon data is sparse (collection just started). This view uses
-- anchor-date logic and will grow more powerful as daily snapshots accumulate.
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_amazon`
--        ORDER BY rank_percentile DESC LIMIT 25

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_amazon` AS

WITH -- Anchor date
anchor AS (
  SELECT MAX(date) as max_date FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`
),

-- Latest snapshot per ASIN (dedup: some ASINs appear multiple times per date)
latest_dedup AS (
  SELECT asin, title, category, author, rank, price_cents, rating, review_count, date,
    ROW_NUMBER() OVER (PARTITION BY asin ORDER BY rank ASC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`, anchor
  WHERE date = anchor.max_date
),
latest AS (
  SELECT * FROM latest_dedup WHERE rn = 1
),

-- Previous date snapshot (for velocity, when available)
prev_date AS (
  SELECT MAX(date) as prev_date
  FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`, anchor
  WHERE date < anchor.max_date
),
prev_dedup AS (
  SELECT asin, rank as prev_rank, price_cents as prev_price,
    ROW_NUMBER() OVER (PARTITION BY asin ORDER BY rank ASC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`, prev_date
  WHERE date = prev_date.prev_date
),
previous AS (
  SELECT asin, prev_rank, prev_price FROM prev_dedup WHERE rn = 1
),

-- ASIN to concept mapping (when available)
asin_map AS (
  SELECT asin, concept_name
  FROM `dnd-trends-index.dnd_trends_categorized.amazon_asin_map`
)

SELECT
  l.asin,
  COALESCE(m.concept_name, l.title) as concept_name,
  l.title,
  l.category,
  l.author,

  -- Rank and percentile
  l.rank as sales_rank,
  ROUND(1.0 - SAFE_DIVIDE(
    CAST(l.rank - 1 AS FLOAT64),
    GREATEST((SELECT COUNT(DISTINCT asin) FROM latest) - 1, 1)
  ), 4) as rank_percentile,

  -- Price
  ROUND(l.price_cents / 100.0, 2) as price_usd,
  -- Price change detection (when prev data exists)
  CASE
    WHEN p.prev_price IS NOT NULL AND l.price_cents != p.prev_price
      THEN ROUND((l.price_cents - p.prev_price) / 100.0, 2)
    ELSE NULL
  END as price_change_usd,

  -- Ratings (community evaluation)
  l.rating,
  l.review_count,

  -- Rank velocity
  CASE
    WHEN p.prev_rank IS NOT NULL THEN p.prev_rank - l.rank
    ELSE NULL
  END as rank_velocity,

  -- Product classification by price tier
  CASE
    WHEN l.price_cents >= 5000 THEN 'premium'
    WHEN l.price_cents >= 2000 THEN 'standard'
    WHEN l.price_cents >= 1000 THEN 'budget'
    ELSE 'impulse'
  END as price_tier,

  -- Rating health
  CASE
    WHEN l.rating >= 4.5 AND l.review_count >= 100 THEN 'acclaimed'
    WHEN l.rating >= 4.0 AND l.review_count >= 50 THEN 'well_received'
    WHEN l.rating >= 3.5 THEN 'mixed'
    WHEN l.rating > 0 THEN 'poor'
    ELSE 'unrated'
  END as rating_health,

  -- Rank tier
  CASE
    WHEN l.rank <= 10 THEN 'top_10'
    WHEN l.rank <= 50 THEN 'top_50'
    WHEN l.rank <= 100 THEN 'top_100'
    ELSE 'tail'
  END as rank_tier,

  -- Standardized output contract
  'retail_demand' as signal_type,
  ROUND(1.0 - SAFE_DIVIDE(
    CAST(l.rank - 1 AS FLOAT64),
    GREATEST((SELECT COUNT(DISTINCT asin) FROM latest) - 1, 1)
  ), 4) as primary_metric,
  ROUND(SAFE_DIVIDE(
    COALESCE(p.prev_rank, l.rank + 1) - l.rank,
    GREATEST(CAST(l.rank AS FLOAT64), 1)
  ), 3) as momentum,
  CASE
    WHEN l.review_count >= 100 AND l.rating > 0 THEN 'HIGH'
    WHEN l.review_count >= 10 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'amazon' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM latest l
LEFT JOIN previous p ON l.asin = p.asin
LEFT JOIN asin_map m ON l.asin = m.asin;
