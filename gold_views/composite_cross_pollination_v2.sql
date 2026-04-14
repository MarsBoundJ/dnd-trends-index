-- Cross-Pollination Index v2: Multi-Stream Concept Reach
-- Dataset: gold_data.composite_cross_pollination_v2
-- Reads from: gold_data.composite_concept_index (materialized daily)
--
-- Upgrade of the Phase 5 cross_pollination_index. Instead of raw signal
-- counts with ad-hoc weighting, this version uses the normalized 5-bucket
-- scores from composite_concept_index and includes the new digital streams
-- (Steam, Twitch, AO3, mods) that the original version didn't capture well.
--
-- Pollination = how many different ecosystem buckets a concept appears in.
-- A concept that only shows up in Google Trends (curiosity) is narrowly
-- pollinated. A concept that appears in curiosity + community + creator +
-- ownership + commerce is fully cross-pollinated — it has penetrated
-- every layer of the D&D ecosystem.
--
-- Usage:
--   -- Most cross-pollinated concepts:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_cross_pollination_v2`
--   WHERE buckets_present >= 4 ORDER BY pollination_score DESC
--
--   -- Concepts ripe for cross-pollination (strong in 1-2 buckets, absent elsewhere):
--   SELECT * FROM `dnd-trends-index.gold_data.composite_cross_pollination_v2`
--   WHERE buckets_present <= 2 AND strongest_bucket_score >= 0.6
--   ORDER BY strongest_bucket_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_cross_pollination_v2` AS

WITH base AS (
  SELECT
    concept_name,
    category,
    ruleset,
    canonical_parent,
    is_active,

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

    curiosity_streams,
    community_streams,
    creator_streams,
    ownership_streams,
    commerce_streams,

    streams_present,
    demand_score,
    supply_score,
    trend_level,
    trend_momentum

  FROM `dnd-trends-index.gold_data.composite_concept_index`
  WHERE streams_present >= 1
),

analyzed AS (
  SELECT
    *,

    -- How many of the 5 buckets have data?
    (CASE WHEN curiosity_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN community_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN creator_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN ownership_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN commerce_score IS NOT NULL THEN 1 ELSE 0 END) AS buckets_present,

    -- Strongest bucket
    GREATEST(
      COALESCE(curiosity_score, 0),
      COALESCE(community_score, 0),
      COALESCE(creator_score, 0),
      COALESCE(ownership_score, 0),
      COALESCE(commerce_score, 0)
    ) AS strongest_bucket_score,

    -- Which bucket is strongest?
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
    END AS strongest_bucket,

    -- Weakest non-null bucket
    LEAST(
      CASE WHEN curiosity_score IS NOT NULL THEN curiosity_score ELSE 1.0 END,
      CASE WHEN community_score IS NOT NULL THEN community_score ELSE 1.0 END,
      CASE WHEN creator_score IS NOT NULL THEN creator_score ELSE 1.0 END,
      CASE WHEN ownership_score IS NOT NULL THEN ownership_score ELSE 1.0 END,
      CASE WHEN commerce_score IS NOT NULL THEN commerce_score ELSE 1.0 END
    ) AS weakest_bucket_score,

    -- Which bucket is weakest (among those present)?
    CASE
      WHEN curiosity_score IS NOT NULL AND COALESCE(curiosity_score, 1) <= LEAST(
             CASE WHEN community_score IS NOT NULL THEN community_score ELSE 1.0 END,
             CASE WHEN creator_score IS NOT NULL THEN creator_score ELSE 1.0 END,
             CASE WHEN ownership_score IS NOT NULL THEN ownership_score ELSE 1.0 END,
             CASE WHEN commerce_score IS NOT NULL THEN commerce_score ELSE 1.0 END)
        THEN 'curiosity'
      WHEN community_score IS NOT NULL AND COALESCE(community_score, 1) <= LEAST(
             CASE WHEN curiosity_score IS NOT NULL THEN curiosity_score ELSE 1.0 END,
             CASE WHEN creator_score IS NOT NULL THEN creator_score ELSE 1.0 END,
             CASE WHEN ownership_score IS NOT NULL THEN ownership_score ELSE 1.0 END,
             CASE WHEN commerce_score IS NOT NULL THEN commerce_score ELSE 1.0 END)
        THEN 'community'
      WHEN creator_score IS NOT NULL AND COALESCE(creator_score, 1) <= LEAST(
             CASE WHEN curiosity_score IS NOT NULL THEN curiosity_score ELSE 1.0 END,
             CASE WHEN community_score IS NOT NULL THEN community_score ELSE 1.0 END,
             CASE WHEN ownership_score IS NOT NULL THEN ownership_score ELSE 1.0 END,
             CASE WHEN commerce_score IS NOT NULL THEN commerce_score ELSE 1.0 END)
        THEN 'creator'
      WHEN ownership_score IS NOT NULL AND COALESCE(ownership_score, 1) <= LEAST(
             CASE WHEN curiosity_score IS NOT NULL THEN curiosity_score ELSE 1.0 END,
             CASE WHEN community_score IS NOT NULL THEN community_score ELSE 1.0 END,
             CASE WHEN creator_score IS NOT NULL THEN creator_score ELSE 1.0 END,
             CASE WHEN commerce_score IS NOT NULL THEN commerce_score ELSE 1.0 END)
        THEN 'ownership'
      ELSE 'commerce'
    END AS weakest_bucket

  FROM base
)

SELECT
  concept_name,
  category,
  ruleset,
  canonical_parent,

  -- =====================================================
  -- POLLINATION METRICS
  -- =====================================================

  -- How many buckets (out of 5) does this concept appear in?
  buckets_present,

  -- Pollination score: breadth × depth
  -- breadth = fraction of buckets present (0-1)
  -- depth = average score across present buckets (0-1)
  -- Result = 0 to 1
  ROUND(
    (buckets_present / 5.0) *
    SAFE_DIVIDE(
      COALESCE(curiosity_score, 0) + COALESCE(community_score, 0) +
      COALESCE(creator_score, 0) + COALESCE(ownership_score, 0) +
      COALESCE(commerce_score, 0),
      buckets_present
    ),
  4) AS pollination_score,

  -- Pollination evenness: how balanced are the bucket scores?
  -- 1.0 = perfectly even across all present buckets, 0.0 = all in one bucket
  ROUND(CASE
    WHEN buckets_present <= 1 THEN 0.0
    ELSE 1.0 - SAFE_DIVIDE(
      strongest_bucket_score - weakest_bucket_score,
      GREATEST(strongest_bucket_score, 0.01)
    )
  END, 4) AS pollination_evenness,

  -- =====================================================
  -- POLLINATION STATUS (upgrade of Phase 5 classification)
  -- =====================================================
  CASE
    -- FULLY POLLINATED: 4+ buckets with meaningful signal
    WHEN buckets_present >= 4
      THEN 'fully_pollinated'
    -- BROADLY POLLINATED: 3 buckets
    WHEN buckets_present = 3
      THEN 'broadly_pollinated'
    -- DUAL POLLINATED: 2 buckets
    WHEN buckets_present = 2
      THEN 'dual_pollinated'
    -- SINGLE STREAM: only 1 bucket
    WHEN buckets_present = 1
      THEN 'single_stream'
    ELSE 'no_signal'
  END AS pollination_status,

  -- Which buckets are MISSING for this concept? (opportunity for expansion)
  CONCAT(
    CASE WHEN curiosity_score IS NULL THEN 'curiosity ' ELSE '' END,
    CASE WHEN community_score IS NULL THEN 'community ' ELSE '' END,
    CASE WHEN creator_score IS NULL THEN 'creator ' ELSE '' END,
    CASE WHEN ownership_score IS NULL THEN 'ownership ' ELSE '' END,
    CASE WHEN commerce_score IS NULL THEN 'commerce ' ELSE '' END
  ) AS missing_buckets,

  -- =====================================================
  -- BUCKET DETAIL
  -- =====================================================
  strongest_bucket,
  ROUND(strongest_bucket_score, 3) AS strongest_bucket_score,
  weakest_bucket,
  ROUND(weakest_bucket_score, 3) AS weakest_bucket_score,

  -- Individual bucket scores
  ROUND(COALESCE(curiosity_score, 0), 3) AS curiosity_score,
  ROUND(COALESCE(community_score, 0), 3) AS community_score,
  ROUND(COALESCE(creator_score, 0), 3) AS creator_score,
  ROUND(COALESCE(ownership_score, 0), 3) AS ownership_score,
  ROUND(COALESCE(commerce_score, 0), 3) AS commerce_score,

  -- Stream counts per bucket
  curiosity_streams,
  community_streams,
  creator_streams,
  ownership_streams,
  commerce_streams,
  streams_present AS total_streams,

  -- =====================================================
  -- CONTEXT
  -- =====================================================
  ROUND(COALESCE(demand_score, 0), 3) AS demand_score,
  ROUND(COALESCE(supply_score, 0), 3) AS supply_score,
  ROUND(COALESCE(trend_level, 0), 3) AS trend_level,
  ROUND(COALESCE(trend_momentum, 0), 3) AS trend_momentum,

  -- Confidence
  CASE
    WHEN buckets_present >= 4 THEN 'HIGH'
    WHEN buckets_present >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS confidence,

  -- =====================================================
  -- RANKS
  -- =====================================================
  ROW_NUMBER() OVER (ORDER BY
    (buckets_present / 5.0) *
    SAFE_DIVIDE(
      COALESCE(curiosity_score, 0) + COALESCE(community_score, 0) +
      COALESCE(creator_score, 0) + COALESCE(ownership_score, 0) +
      COALESCE(commerce_score, 0),
      (CASE WHEN curiosity_score IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN community_score IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN creator_score IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN ownership_score IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN commerce_score IS NOT NULL THEN 1 ELSE 0 END))
    DESC
  ) AS rank_by_pollination,

  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY buckets_present DESC, strongest_bucket_score DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM analyzed;
