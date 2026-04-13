-- Google Trends Analytics: The Curiosity Radar
-- Dataset: gold_data.analytics_google_trends
-- Per-concept search interest analysis with momentum, lifecycle state, and breakout detection
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_google_trends`
--        WHERE lifecycle_state = 'surging' ORDER BY momentum_14d DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_google_trends` AS

WITH mapped_trends AS (
  -- Join raw trend data to concept library mapping
  SELECT
    m.canonical_concept as concept_name,
    m.category,
    m.persona_target,
    t.date,
    AVG(t.interest) as interest  -- avg across multiple term_ids for same concept
  FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
  JOIN `dnd-trends-index.silver_data.view_google_mapping` m ON t.term_id = m.term_id
  GROUP BY m.canonical_concept, m.category, m.persona_target, t.date
),

-- Current window: last 14 days
current_window AS (
  SELECT concept_name, category, persona_target,
    AVG(interest) as avg_interest_14d,
    MAX(interest) as max_interest_14d,
    COUNT(DISTINCT date) as data_points_14d
  FROM mapped_trends
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  GROUP BY concept_name, category, persona_target
),

-- Previous window: 15-28 days ago
previous_window AS (
  SELECT concept_name, category,
    AVG(interest) as avg_interest_prev14d
  FROM mapped_trends
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  GROUP BY concept_name, category
),

-- Full history stats (for percentile-vs-self and volatility)
history_stats AS (
  SELECT concept_name, category,
    AVG(interest) as avg_interest_all,
    STDDEV(interest) as stddev_interest,
    MAX(interest) as max_interest_ever,
    MIN(date) as first_seen,
    MAX(date) as last_seen,
    COUNT(DISTINCT date) as total_data_points,
    COUNTIF(interest > 0) as nonzero_days
  FROM mapped_trends
  GROUP BY concept_name, category
),

-- Breakout detection: interest jumped from <10 to >50 within last 7 days
breakout_check AS (
  SELECT concept_name, category,
    MAX(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN interest ELSE 0 END) as peak_7d,
    AVG(CASE WHEN date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
              AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              THEN interest ELSE NULL END) as baseline_before_7d
  FROM mapped_trends
  GROUP BY concept_name, category
),

-- Assemble everything
assembled AS (
  SELECT
    c.concept_name,
    c.category,
    c.persona_target,

    -- Current interest
    ROUND(c.avg_interest_14d, 1) as current_interest,
    c.max_interest_14d,
    c.data_points_14d,

    -- Momentum: (current - previous) / previous
    ROUND(
      SAFE_DIVIDE(c.avg_interest_14d - COALESCE(p.avg_interest_prev14d, 0),
                  GREATEST(COALESCE(p.avg_interest_prev14d, 0), 1)),
      3
    ) as momentum_14d,

    -- Volatility: coefficient of variation over all history
    ROUND(
      SAFE_DIVIDE(h.stddev_interest, GREATEST(h.avg_interest_all, 0.01)),
      3
    ) as volatility_cv,

    -- Percentile vs own history: where does current 14d avg sit in its own range
    ROUND(
      SAFE_DIVIDE(c.avg_interest_14d - COALESCE(h.avg_interest_all, 0),
                  GREATEST(h.max_interest_ever - COALESCE(h.avg_interest_all, 0), 1)),
      3
    ) as percentile_vs_self,

    -- Breakout flag
    CASE
      WHEN b.peak_7d >= 50 AND COALESCE(b.baseline_before_7d, 0) < 10 THEN TRUE
      ELSE FALSE
    END as is_breakout,

    -- Lifecycle state classification
    CASE
      -- Surging: momentum > 0.3 AND already has meaningful baseline
      WHEN SAFE_DIVIDE(c.avg_interest_14d - COALESCE(p.avg_interest_prev14d, 0),
                       GREATEST(COALESCE(p.avg_interest_prev14d, 0), 1)) > 0.3
           AND COALESCE(h.avg_interest_all, 0) >= 10
        THEN 'surging'
      -- Emerging: momentum > 0.3 AND low baseline (new concept gaining traction)
      WHEN SAFE_DIVIDE(c.avg_interest_14d - COALESCE(p.avg_interest_prev14d, 0),
                       GREATEST(COALESCE(p.avg_interest_prev14d, 0), 1)) > 0.3
           AND COALESCE(h.avg_interest_all, 0) < 10
        THEN 'emerging'
      -- Fading: momentum < -0.2
      WHEN SAFE_DIVIDE(c.avg_interest_14d - COALESCE(p.avg_interest_prev14d, 0),
                       GREATEST(COALESCE(p.avg_interest_prev14d, 0), 1)) < -0.2
        THEN 'fading'
      -- Stable: low volatility
      WHEN SAFE_DIVIDE(h.stddev_interest, GREATEST(h.avg_interest_all, 0.01)) < 0.5
        THEN 'stable'
      -- Dormant: exists but minimal recent activity
      WHEN c.avg_interest_14d < 5 AND h.nonzero_days < h.total_data_points * 0.3
        THEN 'dormant'
      -- Default
      ELSE 'variable'
    END as lifecycle_state,

    -- History context
    h.avg_interest_all as historical_avg,
    h.total_data_points,
    h.nonzero_days,
    h.first_seen,
    h.last_seen,

    -- Rank within category
    ROW_NUMBER() OVER (
      PARTITION BY c.category
      ORDER BY c.avg_interest_14d DESC
    ) as rank_in_category,

    -- Standardized output contract columns
    'curiosity' as signal_type,
    ROUND(c.avg_interest_14d, 1) as primary_metric,
    ROUND(
      SAFE_DIVIDE(c.avg_interest_14d - COALESCE(p.avg_interest_prev14d, 0),
                  GREATEST(COALESCE(p.avg_interest_prev14d, 0), 1)),
      3
    ) as momentum,
    CASE
      WHEN c.data_points_14d >= 10 THEN 'HIGH'
      WHEN c.data_points_14d >= 5 THEN 'MEDIUM'
      ELSE 'LOW'
    END as confidence,
    'google_trends' as stream_name,
    CURRENT_DATE() as snapshot_date

  FROM current_window c
  LEFT JOIN previous_window p ON c.concept_name = p.concept_name AND c.category = p.category
  LEFT JOIN history_stats h ON c.concept_name = h.concept_name AND c.category = h.category
  LEFT JOIN breakout_check b ON c.concept_name = b.concept_name AND c.category = b.category
)

SELECT * FROM assembled
WHERE current_interest > 0 OR is_breakout = TRUE;
