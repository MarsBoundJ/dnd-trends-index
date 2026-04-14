-- Composite Trend Score: The Sortable Leaderboard
-- Dataset: gold_data.composite_trend_score
-- Reads from: gold_data.composite_concept_index (materialized daily)
--
-- Two dimensions per concept:
--   trend_level (0-100)    — where is this concept right now? (weighted absolute score)
--   trend_momentum (0-100) — how fast is it moving? (delta-percentile approach)
--
-- The "one number" leaderboard for sorting concepts, plus a momentum
-- dimension for identifying top movers. Inspired by Perplexity's
-- delta-percentile methodology.
--
-- Weighting (commitment tiers):
--   ownership 2x, commerce 2x — actual commitment signals
--   community 1.5x, creator 1.5x — active engagement signals
--   curiosity 1x — passive interest signals
--
-- Usage:
--   -- Top 25 concepts by overall trend:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_trend_score`
--   WHERE confidence != 'LOW' ORDER BY trend_level DESC LIMIT 25
--
--   -- Top movers (fastest rising):
--   SELECT * FROM `dnd-trends-index.gold_data.composite_trend_score`
--   WHERE confidence != 'LOW' ORDER BY trend_momentum DESC LIMIT 25
--
--   -- Hot AND moving: high level + high momentum
--   SELECT * FROM `dnd-trends-index.gold_data.composite_trend_score`
--   WHERE trend_level >= 50 AND trend_momentum >= 60 ORDER BY combined_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_trend_score` AS

WITH base AS (
  SELECT
    concept_name,
    category,
    ruleset,
    canonical_parent,
    is_active,

    -- Bucket scores and momentum (already 0-1 from the index)
    curiosity_score,
    community_score,
    creator_score,
    ownership_score,
    commerce_score,

    curiosity_momentum,
    community_momentum,
    creator_momentum,
    ownership_momentum,
    commerce_momentum,

    -- Pre-computed from the index
    trend_level AS raw_trend_level,
    trend_momentum AS raw_trend_momentum,
    demand_score,
    supply_score,
    demand_supply_gap,
    talk_to_play_ratio,

    streams_present,
    curiosity_streams,
    community_streams,
    creator_streams,
    ownership_streams,
    commerce_streams

  FROM `dnd-trends-index.gold_data.composite_concept_index`
  WHERE streams_present >= 1
)

SELECT
  concept_name,
  category,
  ruleset,
  canonical_parent,

  -- =====================================================
  -- TREND LEVEL (0-100): where is this concept right now?
  -- Scaled from the 0-1 weighted average in the index
  -- =====================================================
  ROUND(COALESCE(raw_trend_level, 0) * 100, 1) AS trend_level,

  -- =====================================================
  -- TREND MOMENTUM (0-100): how fast is it moving?
  -- >50 = above-average movement, 100 = fastest mover
  -- =====================================================
  ROUND(COALESCE(raw_trend_momentum, 0) * 100, 1) AS trend_momentum,

  -- =====================================================
  -- COMBINED SCORE: level * momentum for "hot AND moving"
  -- Max = 10,000 (100 * 100), useful for finding breakouts
  -- =====================================================
  ROUND(COALESCE(raw_trend_level, 0) * 100 *
        COALESCE(raw_trend_momentum, 0) * 100, 0) AS combined_score,

  -- =====================================================
  -- TREND CLASS: narrative classification
  -- =====================================================
  CASE
    -- SURGING: high level AND high momentum
    WHEN raw_trend_level >= 0.6 AND raw_trend_momentum >= 0.65
      THEN 'surging'
    -- HOT: high level, stable momentum
    WHEN raw_trend_level >= 0.6
      THEN 'hot'
    -- RISING: moderate level but strong momentum (potential breakout)
    WHEN raw_trend_level >= 0.3 AND raw_trend_momentum >= 0.65
      THEN 'rising'
    -- FADING: was strong, momentum declining
    WHEN raw_trend_level >= 0.4 AND raw_trend_momentum < 0.35
      THEN 'fading'
    -- STEADY: moderate level, stable momentum
    WHEN raw_trend_level >= 0.3 AND raw_trend_momentum >= 0.35
      THEN 'steady'
    -- EMERGING: low level but high momentum (early signal)
    WHEN raw_trend_level < 0.3 AND raw_trend_momentum >= 0.7
      THEN 'emerging'
    -- DORMANT: low level, low momentum
    WHEN raw_trend_level < 0.2
      THEN 'dormant'
    ELSE 'neutral'
  END AS trend_class,

  -- =====================================================
  -- BUCKET BREAKDOWN (0-100 scale for readability)
  -- =====================================================
  ROUND(COALESCE(curiosity_score, 0) * 100, 1) AS curiosity,
  ROUND(COALESCE(community_score, 0) * 100, 1) AS community,
  ROUND(COALESCE(creator_score, 0) * 100, 1) AS creator,
  ROUND(COALESCE(ownership_score, 0) * 100, 1) AS ownership,
  ROUND(COALESCE(commerce_score, 0) * 100, 1) AS commerce,

  -- Bucket momentum (0-100)
  ROUND(COALESCE(curiosity_momentum, 0) * 100, 1) AS curiosity_momentum,
  ROUND(COALESCE(community_momentum, 0) * 100, 1) AS community_momentum,
  ROUND(COALESCE(creator_momentum, 0) * 100, 1) AS creator_momentum,
  ROUND(COALESCE(ownership_momentum, 0) * 100, 1) AS ownership_momentum,
  ROUND(COALESCE(commerce_momentum, 0) * 100, 1) AS commerce_momentum,

  -- Dominant bucket: which signal is strongest?
  CASE
    WHEN COALESCE(ownership_score, 0) >= GREATEST(
           COALESCE(curiosity_score, 0), COALESCE(community_score, 0),
           COALESCE(creator_score, 0), COALESCE(commerce_score, 0))
      THEN 'ownership'
    WHEN COALESCE(commerce_score, 0) >= GREATEST(
           COALESCE(curiosity_score, 0), COALESCE(community_score, 0),
           COALESCE(creator_score, 0), COALESCE(ownership_score, 0))
      THEN 'commerce'
    WHEN COALESCE(community_score, 0) >= GREATEST(
           COALESCE(curiosity_score, 0), COALESCE(creator_score, 0),
           COALESCE(ownership_score, 0), COALESCE(commerce_score, 0))
      THEN 'community'
    WHEN COALESCE(creator_score, 0) >= GREATEST(
           COALESCE(curiosity_score, 0), COALESCE(community_score, 0),
           COALESCE(ownership_score, 0), COALESCE(commerce_score, 0))
      THEN 'creator'
    ELSE 'curiosity'
  END AS dominant_bucket,

  -- Pre-computed aggregates
  ROUND(COALESCE(demand_score, 0) * 100, 1) AS demand,
  ROUND(COALESCE(supply_score, 0) * 100, 1) AS supply,
  ROUND(COALESCE(demand_supply_gap, 0) * 100, 1) AS demand_supply_gap,
  ROUND(talk_to_play_ratio, 2) AS talk_to_play_ratio,

  -- Stream breadth
  streams_present,

  -- Confidence
  CASE
    WHEN streams_present >= 5 THEN 'HIGH'
    WHEN streams_present >= 3 THEN 'MEDIUM'
    WHEN streams_present >= 2 THEN 'LOW'
    ELSE 'MINIMAL'
  END AS confidence,

  -- =====================================================
  -- RANKS
  -- =====================================================
  ROW_NUMBER() OVER (ORDER BY COALESCE(raw_trend_level, 0) DESC) AS rank_by_level,
  ROW_NUMBER() OVER (ORDER BY COALESCE(raw_trend_momentum, 0) DESC) AS rank_by_momentum,
  ROW_NUMBER() OVER (ORDER BY COALESCE(raw_trend_level, 0) * COALESCE(raw_trend_momentum, 0) DESC) AS rank_by_combined,
  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY COALESCE(raw_trend_level, 0) DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM base;
