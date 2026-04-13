-- Wikipedia Analytics: The Mainstream Awareness Gauge
-- Dataset: gold_data.analytics_wikipedia
-- Per-article view analysis with momentum, mainstream crossover score, and volatility
--
-- Note: Uses max(date) as anchor instead of CURRENT_DATE() because Wikipedia
-- data may lag behind by a few days.
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_wikipedia`
--        WHERE is_rising = TRUE ORDER BY mainstream_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_wikipedia` AS

WITH -- Anchor date: latest available data
anchor AS (
  SELECT MAX(date) as max_date
  FROM `dnd-trends-index.social_data.wikipedia_daily_views`
),

-- Deduplicated registry (pick first category per article)
registry_dedup AS (
  SELECT article_title, parent_category,
    ROW_NUMBER() OVER (PARTITION BY article_title ORDER BY parent_category) as rn
  FROM `dnd-trends-index.social_data.wikipedia_article_registry`
  WHERE is_tracked = TRUE
),
registry AS (
  SELECT article_title, parent_category FROM registry_dedup WHERE rn = 1
),

-- 7-day window (relative to last available data)
views_7d AS (
  SELECT article_title,
    SUM(views) as views_7d,
    AVG(views) as avg_daily_7d,
    COUNT(DISTINCT date) as days_7d
  FROM `dnd-trends-index.social_data.wikipedia_daily_views`, anchor
  WHERE date > DATE_SUB(anchor.max_date, INTERVAL 7 DAY)
  GROUP BY article_title
),

-- 30-day window
views_30d AS (
  SELECT article_title,
    SUM(views) as views_30d,
    AVG(views) as avg_daily_30d,
    STDDEV(views) as stddev_views_30d,
    MAX(views) as peak_daily_30d,
    COUNT(DISTINCT date) as days_30d
  FROM `dnd-trends-index.social_data.wikipedia_daily_views`, anchor
  WHERE date > DATE_SUB(anchor.max_date, INTERVAL 30 DAY)
  GROUP BY article_title
),

-- Previous 30-day window (31-60 days ago) for longer-term momentum
views_prev30d AS (
  SELECT article_title,
    AVG(views) as avg_daily_prev30d
  FROM `dnd-trends-index.social_data.wikipedia_daily_views`, anchor
  WHERE date > DATE_SUB(anchor.max_date, INTERVAL 60 DAY)
    AND date <= DATE_SUB(anchor.max_date, INTERVAL 30 DAY)
  GROUP BY article_title
)

SELECT
  v30.article_title as concept_name,
  COALESCE(reg.parent_category, 'Uncategorized') as category,

  -- View volumes
  COALESCE(v7.views_7d, 0) as views_7d,
  v30.views_30d,
  ROUND(v30.avg_daily_30d, 1) as avg_daily_views,
  v30.peak_daily_30d,
  v30.days_30d,

  -- Mainstream crossover score: absolute view volume (not percentile)
  -- Wikipedia readers are general public, not just D&D players
  v30.views_30d as mainstream_score,

  -- Momentum ratio: 7d avg / 30d avg — ratio > 1.2 means rising
  ROUND(
    SAFE_DIVIDE(COALESCE(v7.avg_daily_7d, 0), GREATEST(v30.avg_daily_30d, 1)),
    3
  ) as momentum_ratio_7d,

  -- Longer-term momentum: current 30d avg / previous 30d avg
  ROUND(
    SAFE_DIVIDE(v30.avg_daily_30d, GREATEST(COALESCE(vp.avg_daily_prev30d, 0), 1)),
    3
  ) as momentum_ratio_30d,

  -- Volatility: coefficient of variation
  ROUND(
    SAFE_DIVIDE(v30.stddev_views_30d, GREATEST(v30.avg_daily_30d, 1)),
    3
  ) as volatility_cv,

  -- Steady state vs spike classification
  CASE
    WHEN SAFE_DIVIDE(v30.stddev_views_30d, GREATEST(v30.avg_daily_30d, 1)) < 0.5
      THEN 'steady_awareness'
    WHEN SAFE_DIVIDE(v30.stddev_views_30d, GREATEST(v30.avg_daily_30d, 1)) < 1.0
      THEN 'moderate_variation'
    ELSE 'event_driven'
  END as view_pattern,

  -- Rising flag
  CASE
    WHEN SAFE_DIVIDE(COALESCE(v7.avg_daily_7d, 0), GREATEST(v30.avg_daily_30d, 1)) > 1.2
      THEN TRUE
    ELSE FALSE
  END as is_rising,

  -- Falling flag
  CASE
    WHEN SAFE_DIVIDE(COALESCE(v7.avg_daily_7d, 0), GREATEST(v30.avg_daily_30d, 1)) < 0.7
      THEN TRUE
    ELSE FALSE
  END as is_falling,

  -- Rank by absolute views (mainstream reach)
  ROW_NUMBER() OVER (ORDER BY v30.views_30d DESC) as rank_overall,

  -- Rank within category
  ROW_NUMBER() OVER (
    PARTITION BY COALESCE(reg.parent_category, 'Uncategorized')
    ORDER BY v30.views_30d DESC
  ) as rank_in_category,

  -- Standardized output contract
  'mainstream_awareness' as signal_type,
  CAST(v30.views_30d AS FLOAT64) as primary_metric,
  ROUND(
    SAFE_DIVIDE(COALESCE(v7.avg_daily_7d, 0) - v30.avg_daily_30d,
                GREATEST(v30.avg_daily_30d, 1)),
    3
  ) as momentum,
  CASE
    WHEN v30.days_30d >= 20 THEN 'HIGH'
    WHEN v30.days_30d >= 10 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'wikipedia' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM views_30d v30
LEFT JOIN views_7d v7 ON v30.article_title = v7.article_title
LEFT JOIN views_prev30d vp ON v30.article_title = vp.article_title
LEFT JOIN registry reg ON v30.article_title = reg.article_title
WHERE v30.views_30d > 10;
