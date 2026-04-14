-- Creator Economy Dashboard: The 12-18 Month Leading Indicator
-- Dataset: gold_data.composite_creator_economy
-- Reads from: analytics_dmsguild_dtrpg, analytics_itchio, analytics_crowdfunding,
--             analytics_youtube, composite_concept_index
--
-- What independent creators are investing in today is what WotC should invest in
-- 12-18 months from now. This view combines:
--   DMs Guild tier movement  = established creator marketplace
--   Itch.io jam themes       = experimental indie frontier
--   Crowdfunding campaigns   = where creators put their money
--   YouTube creator consensus = what content creators think will perform
--
-- Usage:
--   -- Top creator-endorsed themes:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_creator_economy`
--   WHERE creator_momentum = 'accelerating' ORDER BY creator_investment_score DESC
--
--   -- Emerging themes from Itch.io jams (18-month crystal ball):
--   SELECT * FROM `dnd-trends-index.gold_data.composite_creator_economy`
--   WHERE has_itchio_signal = TRUE ORDER BY itchio_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_creator_economy` AS

WITH -- DMs Guild: medal-tier product performance (established market)
dmsguild_signal AS (
  SELECT
    concept_name,
    source AS marketplace,
    seller_tier,
    tier_rank,
    tier_movement,
    price,
    price_tier,
    -- Normalize: tier-based percentile (Platinum=1.0 down to Normal=0.14)
    primary_metric AS tier_score,
    momentum AS tier_momentum
  FROM `dnd-trends-index.gold_data.analytics_dmsguild_dtrpg`
),

-- Aggregate DMs Guild per concept (a concept may have multiple products)
dmsguild_agg AS (
  SELECT
    concept_name,
    COUNT(*) AS product_count,
    MAX(tier_score) AS best_tier_score,
    MIN(tier_rank) AS best_tier_rank,
    AVG(tier_score) AS avg_tier_score,
    MAX(tier_momentum) AS best_momentum,
    COUNTIF(tier_movement = 'promoted') AS promotions,
    COUNTIF(tier_movement = 'demoted') AS demotions,
    AVG(price) AS avg_price,
    -- Cross-marketplace presence
    COUNT(DISTINCT marketplace) AS marketplace_count,
    COUNTIF(marketplace = 'DMs Guild') AS dmsguild_products,
    COUNTIF(marketplace = 'DriveThruRPG') AS dtrpg_products
  FROM dmsguild_signal
  GROUP BY concept_name
),

-- Itch.io: jam themes and product tags (experimental frontier)
itchio_signal AS (
  SELECT
    concept_name,
    signal_source,
    primary_metric,
    momentum AS itchio_momentum,
    confidence,
    -- Separate jam vs product signals
    CASE WHEN signal_source = 'jam' THEN primary_metric ELSE 0 END AS jam_submissions,
    CASE WHEN signal_source = 'product' THEN primary_metric ELSE 0 END AS product_count,
    CASE WHEN signal_source = 'cluster' THEN primary_metric ELSE 0 END AS cluster_size
  FROM `dnd-trends-index.gold_data.analytics_itchio`
),

itchio_agg AS (
  SELECT
    concept_name,
    SUM(jam_submissions) AS jam_submissions,
    SUM(product_count) AS itchio_products,
    SUM(cluster_size) AS cluster_size,
    MAX(itchio_momentum) AS itchio_momentum,
    MAX(primary_metric) AS itchio_max_metric,
    -- Is this concept appearing in jams (leading indicator) or products (lagging)?
    CASE
      WHEN SUM(jam_submissions) > 0 AND SUM(product_count) > 0 THEN 'jam_and_product'
      WHEN SUM(jam_submissions) > 0 THEN 'jam_only'
      WHEN SUM(product_count) > 0 THEN 'product_only'
      ELSE 'cluster_only'
    END AS itchio_signal_type
  FROM itchio_signal
  GROUP BY concept_name
),

-- Crowdfunding: where creators put real money (highest commitment signal)
crowdfunding_signal AS (
  SELECT
    project_name AS concept_name,
    platform,
    pledged_usd,
    backers_count,
    overfund_pct,
    campaign_archetype,
    is_dnd,
    is_unmet_demand_signal,
    avg_pledge
  FROM `dnd-trends-index.gold_data.analytics_crowdfunding`
  WHERE is_dnd = TRUE  -- focus on D&D-centric campaigns
),

crowdfunding_agg AS (
  SELECT
    concept_name,
    COUNT(*) AS campaign_count,
    SUM(pledged_usd) AS total_pledged,
    SUM(backers_count) AS total_backers,
    MAX(overfund_pct) AS max_overfund,
    AVG(avg_pledge) AS avg_pledge_per_backer,
    COUNTIF(is_unmet_demand_signal = TRUE) AS unmet_demand_signals,
    STRING_AGG(DISTINCT campaign_archetype, ', ') AS archetypes
  FROM crowdfunding_signal
  GROUP BY concept_name
),

-- YouTube: creator consensus signal (what creators think matters)
youtube_signal AS (
  SELECT
    concept_name,
    creator_count,
    video_count,
    videos_30d,
    consensus_score,
    dominant_sentiment,
    has_sentiment_divergence,
    tier1_creators,
    primary_metric AS yt_creator_score,
    momentum AS yt_momentum
  FROM `dnd-trends-index.gold_data.analytics_youtube`
),

-- Composite concept index scores (for context)
idx AS (
  SELECT
    concept_name,
    category,
    ruleset,
    demand_score,
    supply_score,
    trend_level,
    streams_present
  FROM `dnd-trends-index.gold_data.composite_concept_index`
),

-- Combine all creator signals
combined AS (
  SELECT
    COALESCE(d.concept_name, i.concept_name, c.concept_name, y.concept_name) AS concept_name,

    -- DMs Guild signals
    d.product_count AS dmsguild_product_count,
    d.best_tier_score,
    d.best_tier_rank,
    d.avg_tier_score,
    d.promotions,
    d.demotions,
    d.marketplace_count,
    d.avg_price AS dmsguild_avg_price,

    -- Itch.io signals
    i.jam_submissions,
    i.itchio_products,
    i.cluster_size,
    i.itchio_momentum,
    i.itchio_signal_type,

    -- Crowdfunding signals
    c.campaign_count,
    c.total_pledged,
    c.total_backers,
    c.max_overfund,
    c.avg_pledge_per_backer,
    c.unmet_demand_signals,
    c.archetypes AS crowdfunding_archetypes,

    -- YouTube signals
    y.creator_count AS yt_creators,
    y.video_count AS yt_videos,
    y.videos_30d AS yt_videos_30d,
    y.consensus_score AS yt_consensus,
    y.dominant_sentiment AS yt_sentiment,
    y.tier1_creators AS yt_tier1_creators,

    -- Flags
    CASE WHEN d.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_marketplace_signal,
    CASE WHEN i.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_itchio_signal,
    CASE WHEN c.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_crowdfunding_signal,
    CASE WHEN y.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_youtube_signal

  FROM dmsguild_agg d
  FULL OUTER JOIN itchio_agg i ON LOWER(d.concept_name) = LOWER(i.concept_name)
  FULL OUTER JOIN crowdfunding_agg c
    ON LOWER(COALESCE(d.concept_name, i.concept_name)) = LOWER(c.concept_name)
  FULL OUTER JOIN youtube_signal y
    ON LOWER(COALESCE(d.concept_name, i.concept_name, c.concept_name)) = LOWER(y.concept_name)
)

SELECT
  cb.concept_name,
  idx.category,
  idx.ruleset,

  -- =====================================================
  -- CREATOR INVESTMENT SCORE (0-1)
  -- Weighted: Crowdfunding 30%, DMs Guild 25%, YouTube 25%, Itch.io 20%
  -- =====================================================
  ROUND(
    -- Crowdfunding (30%): normalized by max total_pledged across all concepts
    COALESCE(SAFE_DIVIDE(cb.total_pledged,
      (SELECT MAX(total_pledged) FROM crowdfunding_agg)), 0) * 0.30 +
    -- DMs Guild (25%): best tier score (already 0-1)
    COALESCE(cb.best_tier_score, 0) * 0.25 +
    -- YouTube (25%): normalized creator count
    COALESCE(SAFE_DIVIDE(CAST(cb.yt_creators AS FLOAT64),
      (SELECT MAX(creator_count) FROM youtube_signal)), 0) * 0.25 +
    -- Itch.io (20%): normalized max metric
    COALESCE(SAFE_DIVIDE(COALESCE(cb.jam_submissions, 0) + COALESCE(cb.itchio_products, 0),
      (SELECT MAX(jam_submissions + itchio_products) FROM itchio_agg)), 0) * 0.20
  , 4) AS creator_investment_score,

  -- Creator momentum: is investment accelerating?
  CASE
    WHEN COALESCE(cb.promotions, 0) > COALESCE(cb.demotions, 0)
     AND COALESCE(cb.itchio_momentum, 0) > 0
      THEN 'accelerating'
    WHEN COALESCE(cb.promotions, 0) > 0
     OR COALESCE(cb.itchio_momentum, 0) > 0
      THEN 'growing'
    WHEN COALESCE(cb.demotions, 0) > COALESCE(cb.promotions, 0)
      THEN 'declining'
    ELSE 'stable'
  END AS creator_momentum,

  -- Leading indicator classification
  CASE
    -- FRONTIER: Itch.io jam activity but no marketplace products yet
    WHEN cb.has_itchio_signal = TRUE
     AND cb.has_marketplace_signal = FALSE
     AND cb.has_crowdfunding_signal = FALSE
      THEN 'frontier'
    -- GESTATING: Itch.io + crowdfunding but not yet in marketplace
    WHEN (cb.has_itchio_signal = TRUE OR cb.has_crowdfunding_signal = TRUE)
     AND cb.has_marketplace_signal = FALSE
      THEN 'gestating'
    -- VALIDATED: Present across multiple creator channels
    WHEN (CASE WHEN cb.has_marketplace_signal THEN 1 ELSE 0 END +
          CASE WHEN cb.has_itchio_signal THEN 1 ELSE 0 END +
          CASE WHEN cb.has_crowdfunding_signal THEN 1 ELSE 0 END +
          CASE WHEN cb.has_youtube_signal THEN 1 ELSE 0 END) >= 3
      THEN 'validated'
    -- MARKETPLACE ONLY: established but no indie frontier signal
    WHEN cb.has_marketplace_signal = TRUE
     AND cb.has_itchio_signal = FALSE
      THEN 'marketplace_established'
    ELSE 'emerging'
  END AS maturity_stage,

  -- Source counts
  (CASE WHEN cb.has_marketplace_signal THEN 1 ELSE 0 END +
   CASE WHEN cb.has_itchio_signal THEN 1 ELSE 0 END +
   CASE WHEN cb.has_crowdfunding_signal THEN 1 ELSE 0 END +
   CASE WHEN cb.has_youtube_signal THEN 1 ELSE 0 END) AS creator_sources_present,

  -- Detail: marketplace
  cb.dmsguild_product_count,
  cb.best_tier_rank,
  cb.promotions,
  cb.demotions,
  cb.marketplace_count,
  cb.dmsguild_avg_price,

  -- Detail: Itch.io
  cb.jam_submissions,
  cb.itchio_products,
  cb.itchio_signal_type,
  cb.itchio_momentum,

  -- Detail: Crowdfunding
  cb.campaign_count,
  ROUND(cb.total_pledged, 2) AS total_pledged,
  cb.total_backers,
  ROUND(cb.max_overfund, 1) AS max_overfund_pct,
  cb.unmet_demand_signals,
  cb.crowdfunding_archetypes,

  -- Detail: YouTube
  cb.yt_creators,
  cb.yt_videos,
  cb.yt_videos_30d,
  ROUND(cb.yt_consensus, 2) AS yt_consensus_score,
  cb.yt_sentiment,
  cb.yt_tier1_creators,

  -- Flags
  cb.has_marketplace_signal,
  cb.has_itchio_signal,
  cb.has_crowdfunding_signal,
  cb.has_youtube_signal,

  -- Context from composite index
  idx.demand_score,
  idx.supply_score,
  idx.trend_level,
  idx.streams_present AS total_streams_present,

  -- Rank
  ROW_NUMBER() OVER (ORDER BY
    COALESCE(SAFE_DIVIDE(cb.total_pledged,
      (SELECT MAX(total_pledged) FROM crowdfunding_agg)), 0) * 0.30 +
    COALESCE(cb.best_tier_score, 0) * 0.25 +
    COALESCE(SAFE_DIVIDE(CAST(cb.yt_creators AS FLOAT64),
      (SELECT MAX(creator_count) FROM youtube_signal)), 0) * 0.25 +
    COALESCE(SAFE_DIVIDE(COALESCE(cb.jam_submissions, 0) + COALESCE(cb.itchio_products, 0),
      (SELECT MAX(jam_submissions + itchio_products) FROM itchio_agg)), 0) * 0.20
    DESC
  ) AS rank_overall,

  CURRENT_DATE() AS snapshot_date

FROM combined cb
LEFT JOIN idx ON LOWER(cb.concept_name) = LOWER(idx.concept_name);
