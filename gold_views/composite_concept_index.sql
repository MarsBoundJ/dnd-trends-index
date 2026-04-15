-- Composite Concept Index: The Normalization Foundation
-- Dataset: gold_data.composite_concept_index
-- Type: MATERIALIZED TABLE (refresh daily via scheduled query)
--
-- Normalizes all 13 single-stream analytics views into 5 strategic buckets.
-- Every downstream composite reads from this table instead of hitting 13 views.
--
-- Bucket assignments:
--   curiosity_score  = Google Trends + Wikipedia + Twitch (passive interest)
--   community_score  = Reddit + Fandom Wiki (active discussion/prep)
--   creator_score    = YouTube + Itch.io + AO3 + mod.io + Nexus Mods (content creation)
--   ownership_score  = BGG/RPGGeek + Roll20 + Steam (actual play/ownership)
--   commerce_score   = Crowdfunding + Amazon + DMs Guild + DriveThruRPG + DDB Catalog (money spent/supply)
--
-- Normalization: PERCENT_RANK() within each stream converts heterogeneous
-- metrics (Google interest 0-100, Reddit mention counts, Amazon rank percentile,
-- Kickstarter USD, etc.) to a common 0-1 scale.
--
-- Confidence weighting: HIGH=1.0, MEDIUM=0.7, LOW=0.4.
-- A low-confidence signal contributes less to the bucket average.
--
-- Usage:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_concept_index`
--   WHERE streams_present >= 3 ORDER BY demand_score DESC
--
--   -- Blue ocean candidates:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_concept_index`
--   WHERE demand_score > 0.6 AND supply_score < 0.4

CREATE OR REPLACE TABLE `dnd-trends-index.gold_data.composite_concept_index` AS

WITH -- =====================================================================
-- STEP 1: UNION all 13 analytics views into one pool (contract columns only)
-- =====================================================================
all_streams AS (
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_google_trends`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_reddit`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_youtube`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_fandom`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_wikipedia`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_bgg`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_roll20`
  UNION ALL
  SELECT project_name AS concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_crowdfunding`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_amazon`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_itchio`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_digital_gaming`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_ddb_catalog`
  UNION ALL
  SELECT concept_name, signal_type, primary_metric, momentum, confidence, stream_name
  FROM `dnd-trends-index.gold_data.analytics_dmsguild_dtrpg`
),

-- =====================================================================
-- STEP 2: Normalize metrics to 0-1 within each stream via PERCENT_RANK
-- =====================================================================
normalized AS (
  SELECT
    concept_name,
    stream_name,
    primary_metric,
    momentum,
    -- Normalize primary_metric: 0 = weakest in this stream, 1 = strongest
    PERCENT_RANK() OVER (
      PARTITION BY stream_name ORDER BY primary_metric ASC
    ) AS norm_metric,
    -- Normalize momentum: 0 = most negative movement, 1 = most positive
    PERCENT_RANK() OVER (
      PARTITION BY stream_name ORDER BY momentum ASC
    ) AS norm_momentum,
    -- Convert confidence label to numeric weight
    CASE confidence
      WHEN 'HIGH' THEN 1.0
      WHEN 'MEDIUM' THEN 0.7
      WHEN 'LOW' THEN 0.4
      ELSE 0.5
    END AS confidence_weight,
    -- Map stream to strategic bucket
    CASE stream_name
      -- Curiosity: passive interest / awareness
      WHEN 'google_trends' THEN 'curiosity'
      WHEN 'wikipedia'     THEN 'curiosity'
      WHEN 'twitch'        THEN 'curiosity'
      -- Community: active discussion and prep
      WHEN 'reddit'        THEN 'community'
      WHEN 'fandom_wiki'   THEN 'community'
      -- Creator: content production
      WHEN 'youtube'       THEN 'creator'
      WHEN 'itchio'        THEN 'creator'
      WHEN 'ao3'           THEN 'creator'
      WHEN 'modio'         THEN 'creator'
      WHEN 'nexus'         THEN 'creator'
      -- Ownership: actual play / material possession
      WHEN 'bgg_rpggeek'   THEN 'ownership'
      WHEN 'roll20'        THEN 'ownership'
      WHEN 'steam'         THEN 'ownership'
      -- Commerce: money spent / supply available
      WHEN 'crowdfunding'  THEN 'commerce'
      WHEN 'amazon'        THEN 'commerce'
      WHEN 'dmsguild'      THEN 'commerce'
      WHEN 'dtrpg'         THEN 'commerce'
      WHEN 'ddb_catalog'   THEN 'commerce'
      ELSE 'other'
    END AS bucket
  FROM all_streams
  WHERE primary_metric IS NOT NULL
),

-- =====================================================================
-- STEP 3: Confidence-weighted average per concept per bucket
-- =====================================================================
bucket_scores AS (
  SELECT
    concept_name,
    bucket,
    -- Confidence-weighted average of normalized metrics
    ROUND(SAFE_DIVIDE(
      SUM(norm_metric * confidence_weight),
      SUM(confidence_weight)
    ), 4) AS bucket_score,
    -- Confidence-weighted momentum
    ROUND(SAFE_DIVIDE(
      SUM(norm_momentum * confidence_weight),
      SUM(confidence_weight)
    ), 4) AS bucket_momentum,
    -- How many distinct streams contributed to this bucket
    COUNT(DISTINCT stream_name) AS streams_in_bucket,
    -- Average confidence across contributing streams
    ROUND(AVG(confidence_weight), 2) AS avg_confidence,
    -- Best single stream in this bucket (for drill-down)
    MAX(stream_name) AS top_stream
  FROM normalized
  GROUP BY concept_name, bucket
),

-- =====================================================================
-- STEP 4: Pivot buckets into columns (one row per concept)
-- =====================================================================
pivoted AS (
  SELECT
    concept_name,

    -- Curiosity bucket
    MAX(CASE WHEN bucket = 'curiosity' THEN bucket_score END)       AS curiosity_score,
    MAX(CASE WHEN bucket = 'curiosity' THEN bucket_momentum END)    AS curiosity_momentum,
    MAX(CASE WHEN bucket = 'curiosity' THEN streams_in_bucket END)  AS curiosity_streams,
    MAX(CASE WHEN bucket = 'curiosity' THEN avg_confidence END)     AS curiosity_avg_confidence,

    -- Community bucket
    MAX(CASE WHEN bucket = 'community' THEN bucket_score END)       AS community_score,
    MAX(CASE WHEN bucket = 'community' THEN bucket_momentum END)    AS community_momentum,
    MAX(CASE WHEN bucket = 'community' THEN streams_in_bucket END)  AS community_streams,
    MAX(CASE WHEN bucket = 'community' THEN avg_confidence END)     AS community_avg_confidence,

    -- Creator bucket
    MAX(CASE WHEN bucket = 'creator' THEN bucket_score END)         AS creator_score,
    MAX(CASE WHEN bucket = 'creator' THEN bucket_momentum END)      AS creator_momentum,
    MAX(CASE WHEN bucket = 'creator' THEN streams_in_bucket END)    AS creator_streams,
    MAX(CASE WHEN bucket = 'creator' THEN avg_confidence END)       AS creator_avg_confidence,

    -- Ownership bucket
    MAX(CASE WHEN bucket = 'ownership' THEN bucket_score END)       AS ownership_score,
    MAX(CASE WHEN bucket = 'ownership' THEN bucket_momentum END)    AS ownership_momentum,
    MAX(CASE WHEN bucket = 'ownership' THEN streams_in_bucket END)  AS ownership_streams,
    MAX(CASE WHEN bucket = 'ownership' THEN avg_confidence END)     AS ownership_avg_confidence,

    -- Commerce bucket
    MAX(CASE WHEN bucket = 'commerce' THEN bucket_score END)        AS commerce_score,
    MAX(CASE WHEN bucket = 'commerce' THEN bucket_momentum END)     AS commerce_momentum,
    MAX(CASE WHEN bucket = 'commerce' THEN streams_in_bucket END)   AS commerce_streams,
    MAX(CASE WHEN bucket = 'commerce' THEN avg_confidence END)      AS commerce_avg_confidence
  FROM bucket_scores
  GROUP BY concept_name
)

-- =====================================================================
-- STEP 5: Final assembly with concept library metadata + composite scores
-- =====================================================================
SELECT
  p.concept_name,

  -- Concept library metadata (NULL if concept not in library, e.g. mod names)
  cl.category,
  cl.ruleset,
  cl.canonical_parent,
  cl.is_active,

  -- ---------------------------------------------------------------
  -- 5 Bucket scores (0-1 scale, NULL = no data in that bucket)
  -- ---------------------------------------------------------------
  p.curiosity_score,
  p.community_score,
  p.creator_score,
  p.ownership_score,
  p.commerce_score,

  -- ---------------------------------------------------------------
  -- 5 Bucket momentum (0-1 percentile; >0.5 = above-average movement)
  -- ---------------------------------------------------------------
  p.curiosity_momentum,
  p.community_momentum,
  p.creator_momentum,
  p.ownership_momentum,
  p.commerce_momentum,

  -- ---------------------------------------------------------------
  -- Stream presence per bucket
  -- ---------------------------------------------------------------
  COALESCE(p.curiosity_streams, 0)  AS curiosity_streams,
  COALESCE(p.community_streams, 0)  AS community_streams,
  COALESCE(p.creator_streams, 0)    AS creator_streams,
  COALESCE(p.ownership_streams, 0)  AS ownership_streams,
  COALESCE(p.commerce_streams, 0)   AS commerce_streams,

  -- ---------------------------------------------------------------
  -- Per-bucket average stream confidence weight (HIGH=1.0/MED=0.7/LOW=0.4)
  -- NULL when the bucket has no data. Added 2026-04-15 to power
  -- the Step 6 concept_confidence view (§5.1 data layer of the
  -- Arcane Analytics confidence formula).
  -- ---------------------------------------------------------------
  p.curiosity_avg_confidence,
  p.community_avg_confidence,
  p.creator_avg_confidence,
  p.ownership_avg_confidence,
  p.commerce_avg_confidence,

  -- Total distinct streams that have data for this concept
  COALESCE(p.curiosity_streams, 0) +
  COALESCE(p.community_streams, 0) +
  COALESCE(p.creator_streams, 0)   +
  COALESCE(p.ownership_streams, 0) +
  COALESCE(p.commerce_streams, 0)  AS streams_present,

  -- ---------------------------------------------------------------
  -- Pre-computed aggregate scores for downstream composites
  -- ---------------------------------------------------------------

  -- DEMAND SCORE: avg of non-null curiosity + community scores
  -- "How much does the audience want this concept?"
  ROUND(SAFE_DIVIDE(
    COALESCE(p.curiosity_score, 0) + COALESCE(p.community_score, 0),
    (CASE WHEN p.curiosity_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.community_score IS NOT NULL THEN 1 ELSE 0 END)
  ), 4) AS demand_score,

  -- SUPPLY SCORE: avg of non-null commerce + ownership scores
  -- "How much supply/product exists for this concept?"
  ROUND(SAFE_DIVIDE(
    COALESCE(p.commerce_score, 0) + COALESCE(p.ownership_score, 0),
    (CASE WHEN p.commerce_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.ownership_score IS NOT NULL THEN 1 ELSE 0 END)
  ), 4) AS supply_score,

  -- DEMAND-SUPPLY GAP: positive = more demand than supply (opportunity)
  ROUND(
    SAFE_DIVIDE(
      COALESCE(p.curiosity_score, 0) + COALESCE(p.community_score, 0),
      (CASE WHEN p.curiosity_score IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN p.community_score IS NOT NULL THEN 1 ELSE 0 END)
    ) -
    SAFE_DIVIDE(
      COALESCE(p.commerce_score, 0) + COALESCE(p.ownership_score, 0),
      (CASE WHEN p.commerce_score IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN p.ownership_score IS NOT NULL THEN 1 ELSE 0 END)
    ),
  4) AS demand_supply_gap,

  -- TALK-TO-PLAY RATIO: community / ownership
  -- >1 = "all talk", <1 = "hidden gem", NULL = missing data
  ROUND(SAFE_DIVIDE(
    p.community_score,
    GREATEST(p.ownership_score, 0.01)
  ), 4) AS talk_to_play_ratio,

  -- OVERALL TREND LEVEL: weighted average of all buckets (commitment tiers)
  -- Weights: ownership 2x, commerce 2x, community 1.5x, creator 1.5x, curiosity 1x
  ROUND(SAFE_DIVIDE(
    COALESCE(p.curiosity_score, 0) * 1.0 * (CASE WHEN p.curiosity_score IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.community_score, 0) * 1.5 * (CASE WHEN p.community_score IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.creator_score, 0)   * 1.5 * (CASE WHEN p.creator_score IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.ownership_score, 0) * 2.0 * (CASE WHEN p.ownership_score IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.commerce_score, 0)  * 2.0 * (CASE WHEN p.commerce_score IS NOT NULL THEN 1 ELSE 0 END),
    1.0 * (CASE WHEN p.curiosity_score IS NOT NULL THEN 1 ELSE 0 END) +
    1.5 * (CASE WHEN p.community_score IS NOT NULL THEN 1 ELSE 0 END) +
    1.5 * (CASE WHEN p.creator_score IS NOT NULL THEN 1 ELSE 0 END) +
    2.0 * (CASE WHEN p.ownership_score IS NOT NULL THEN 1 ELSE 0 END) +
    2.0 * (CASE WHEN p.commerce_score IS NOT NULL THEN 1 ELSE 0 END)
  ), 4) AS trend_level,

  -- OVERALL TREND MOMENTUM: same weighting applied to momentum percentiles
  ROUND(SAFE_DIVIDE(
    COALESCE(p.curiosity_momentum, 0) * 1.0 * (CASE WHEN p.curiosity_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.community_momentum, 0) * 1.5 * (CASE WHEN p.community_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.creator_momentum, 0)   * 1.5 * (CASE WHEN p.creator_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.ownership_momentum, 0) * 2.0 * (CASE WHEN p.ownership_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    COALESCE(p.commerce_momentum, 0)  * 2.0 * (CASE WHEN p.commerce_momentum IS NOT NULL THEN 1 ELSE 0 END),
    1.0 * (CASE WHEN p.curiosity_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    1.5 * (CASE WHEN p.community_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    1.5 * (CASE WHEN p.creator_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    2.0 * (CASE WHEN p.ownership_momentum IS NOT NULL THEN 1 ELSE 0 END) +
    2.0 * (CASE WHEN p.commerce_momentum IS NOT NULL THEN 1 ELSE 0 END)
  ), 4) AS trend_momentum,

  -- Snapshot metadata
  CURRENT_DATE() AS snapshot_date

FROM pivoted p
LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` cl
  ON LOWER(p.concept_name) = LOWER(cl.concept_name)
  AND (cl.is_active = TRUE OR cl.is_active IS NULL);
