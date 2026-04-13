-- Roll20 Analytics: The "Actually Played" Signal
-- Dataset: gold_data.analytics_roll20
-- Per-product VTT adoption, rank velocity, and staying power analysis
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_roll20`
--        WHERE staying_power_weeks >= 4 ORDER BY play_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_roll20` AS

WITH -- Latest snapshot
latest AS (
  SELECT title, publisher, category, rank, price, snapshot_date
  FROM `dnd-trends-index.commercial_data.roll20_rankings`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `dnd-trends-index.commercial_data.roll20_rankings`)
),

-- Previous snapshot (for velocity)
prev_snapshot_date AS (
  SELECT MAX(snapshot_date) as prev_date
  FROM `dnd-trends-index.commercial_data.roll20_rankings`
  WHERE snapshot_date < (SELECT MAX(snapshot_date) FROM `dnd-trends-index.commercial_data.roll20_rankings`)
),
previous AS (
  SELECT title, rank as prev_rank
  FROM `dnd-trends-index.commercial_data.roll20_rankings`
  WHERE snapshot_date = (SELECT prev_date FROM prev_snapshot_date)
),

-- Staying power: count of distinct snapshots where title appears in Top 20
top20_appearances AS (
  SELECT title,
    COUNT(DISTINCT snapshot_date) as weeks_in_top20
  FROM `dnd-trends-index.commercial_data.roll20_rankings`
  WHERE rank <= 20
  GROUP BY title
),

-- All-time appearances (for consistency metric)
all_appearances AS (
  SELECT title,
    COUNT(DISTINCT snapshot_date) as total_appearances,
    MIN(rank) as best_rank_ever,
    AVG(rank) as avg_rank
  FROM `dnd-trends-index.commercial_data.roll20_rankings`
  GROUP BY title
),

-- Publisher market share in latest snapshot
publisher_share AS (
  SELECT publisher,
    COUNT(*) as titles_in_top200,
    COUNTIF(rank <= 20) as titles_in_top20
  FROM latest
  GROUP BY publisher
),

-- WotC flag
wotc_check AS (
  SELECT title,
    CASE WHEN publisher = 'Wizards of the Coast' THEN TRUE ELSE FALSE END as is_wotc
  FROM latest
)

SELECT
  l.title as concept_name,
  l.category,
  l.publisher,
  w.is_wotc,

  -- Current rank and play score
  l.rank as roll20_rank,
  -- Play score: inverted percentile (rank 1 = 1.0, rank 200 = ~0)
  ROUND(1.0 - SAFE_DIVIDE(l.rank - 1, 199), 4) as play_score,
  l.price,

  -- Rank velocity: positive = climbing, negative = falling
  COALESCE(p.prev_rank, 201) - l.rank as rank_velocity,
  p.prev_rank,

  -- Staying power
  COALESCE(t20.weeks_in_top20, 0) as staying_power_weeks,

  -- Consistency
  a.total_appearances,
  a.best_rank_ever,
  ROUND(a.avg_rank, 1) as avg_rank_historical,

  -- Publisher competitive context
  ps.titles_in_top200 as publisher_catalog_size,
  ps.titles_in_top20 as publisher_top20_count,

  -- Product classification
  CASE
    -- Evergreen: consistently in top 20 across most snapshots
    WHEN COALESCE(t20.weeks_in_top20, 0) >= 4 AND l.rank <= 20
      THEN 'evergreen'
    -- Rising star: big rank jump and now in top 50
    WHEN COALESCE(p.prev_rank, 201) - l.rank >= 20 AND l.rank <= 50
      THEN 'rising_star'
    -- Falling: was top 20, now dropped significantly
    WHEN COALESCE(t20.weeks_in_top20, 0) >= 2 AND l.rank > 50
      THEN 'fading'
    -- Launch spike: appeared recently, high rank, few appearances
    WHEN a.total_appearances <= 2 AND l.rank <= 30
      THEN 'launch_spike'
    -- Long tail: consistently present but never top 20
    WHEN a.total_appearances >= 4 AND COALESCE(t20.weeks_in_top20, 0) = 0
      THEN 'long_tail'
    ELSE 'mid_tier'
  END as product_lifecycle,

  -- Rank tiers
  CASE
    WHEN l.rank <= 10 THEN 'top_10'
    WHEN l.rank <= 20 THEN 'top_20'
    WHEN l.rank <= 50 THEN 'top_50'
    WHEN l.rank <= 100 THEN 'top_100'
    ELSE 'tail'
  END as rank_tier,

  -- Standardized output contract
  'play' as signal_type,
  ROUND(1.0 - SAFE_DIVIDE(l.rank - 1, 199), 4) as primary_metric,
  ROUND(SAFE_DIVIDE(
    COALESCE(p.prev_rank, 201) - l.rank,
    GREATEST(l.rank, 1)
  ), 3) as momentum,
  CASE
    WHEN a.total_appearances >= 5 THEN 'HIGH'
    WHEN a.total_appearances >= 3 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'roll20' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM latest l
LEFT JOIN previous p ON l.title = p.title
LEFT JOIN top20_appearances t20 ON l.title = t20.title
LEFT JOIN all_appearances a ON l.title = a.title
LEFT JOIN publisher_share ps ON l.publisher = ps.publisher
LEFT JOIN wotc_check w ON l.title = w.title;
