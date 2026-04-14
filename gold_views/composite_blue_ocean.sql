-- Blue Ocean Matrix: High Demand, Low Supply Opportunities
-- Dataset: gold_data.composite_blue_ocean
-- Reads from: gold_data.composite_concept_index (materialized daily)
--
-- The core WotC question: "What do players want that we haven't published yet?"
--
-- Demand = curiosity_score + community_score (people searching and talking)
-- Supply = commerce_score + ownership_score (products available and owned)
--
-- Blue ocean = high demand + low supply = unmet market opportunity
-- Red ocean  = high demand + high supply = saturated competitive space
-- Dead zone  = low demand + low supply   = nobody cares, nothing exists
-- Over-supplied = low demand + high supply = WotC over-invested
--
-- Usage:
--   -- Top blue ocean opportunities:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_blue_ocean`
--   WHERE quadrant = 'blue_ocean' ORDER BY opportunity_score DESC
--
--   -- Over-supplied (WotC should reconsider):
--   SELECT * FROM `dnd-trends-index.gold_data.composite_blue_ocean`
--   WHERE quadrant = 'over_supplied' ORDER BY demand_supply_gap ASC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_blue_ocean` AS

WITH indexed AS (
  SELECT
    concept_name,
    category,
    ruleset,
    canonical_parent,

    -- Core scores from the index
    curiosity_score,
    community_score,
    creator_score,
    ownership_score,
    commerce_score,

    -- Pre-computed aggregates
    demand_score,
    supply_score,
    demand_supply_gap,

    -- Momentum
    curiosity_momentum,
    community_momentum,
    ownership_momentum,
    commerce_momentum,

    -- Stream breadth
    streams_present,
    curiosity_streams,
    community_streams,
    ownership_streams,
    commerce_streams,

    trend_level,
    trend_momentum

  FROM `dnd-trends-index.gold_data.composite_concept_index`
  -- Require at least 1 demand stream AND exclude noise with no real signal
  WHERE (curiosity_score IS NOT NULL OR community_score IS NOT NULL)
    AND streams_present >= 2
),

-- Quadrant thresholds: demand > 0.6 is "high", supply < 0.4 is "low"
classified AS (
  SELECT
    *,

    -- Quadrant classification
    CASE
      WHEN demand_score >= 0.6 AND supply_score < 0.4  THEN 'blue_ocean'
      WHEN demand_score >= 0.6 AND supply_score >= 0.4 THEN 'red_ocean'
      WHEN demand_score < 0.6  AND supply_score >= 0.4 THEN 'over_supplied'
      ELSE 'dead_zone'
    END AS quadrant,

    -- Opportunity score: how strong is the blue ocean signal?
    -- Combines gap magnitude, demand strength, and momentum
    ROUND(
      GREATEST(COALESCE(demand_supply_gap, 0), 0) *                       -- gap (0 to ~1)
      COALESCE(demand_score, 0) *                                          -- demand strength
      (1.0 + GREATEST(COALESCE(curiosity_momentum, 0.5) - 0.5, 0) * 2.0)  -- momentum boost
    , 4) AS opportunity_score,

    -- Supply gap detail: which supply channels are missing?
    CASE
      WHEN commerce_score IS NULL AND ownership_score IS NULL THEN 'no_supply_data'
      WHEN commerce_score IS NULL THEN 'no_commerce'
      WHEN ownership_score IS NULL THEN 'no_ownership'
      WHEN commerce_score < 0.2 AND ownership_score < 0.2 THEN 'minimal_supply'
      WHEN commerce_score < ownership_score THEN 'commerce_gap'
      ELSE 'ownership_gap'
    END AS supply_gap_type,

    -- Demand composition: what's driving demand?
    CASE
      WHEN curiosity_score IS NOT NULL AND community_score IS NOT NULL
        THEN CASE
          WHEN curiosity_score > community_score * 1.5 THEN 'search_driven'
          WHEN community_score > curiosity_score * 1.5 THEN 'community_driven'
          ELSE 'balanced_demand'
        END
      WHEN curiosity_score IS NOT NULL THEN 'search_only'
      WHEN community_score IS NOT NULL THEN 'community_only'
      ELSE 'unknown'
    END AS demand_driver,

    -- Creator signal: are indie creators already filling this gap?
    CASE
      WHEN creator_score IS NOT NULL AND creator_score >= 0.6 THEN 'creators_active'
      WHEN creator_score IS NOT NULL AND creator_score >= 0.3 THEN 'creators_emerging'
      ELSE 'creators_absent'
    END AS creator_fill_status

  FROM indexed
)

SELECT
  concept_name,
  category,
  ruleset,
  canonical_parent,
  quadrant,

  -- Scores
  ROUND(demand_score, 3)  AS demand_score,
  ROUND(supply_score, 3)  AS supply_score,
  ROUND(demand_supply_gap, 3) AS demand_supply_gap,
  opportunity_score,

  -- Bucket detail
  curiosity_score,
  community_score,
  creator_score,
  ownership_score,
  commerce_score,

  -- Analysis
  demand_driver,
  supply_gap_type,
  creator_fill_status,

  -- Momentum context: is demand rising or falling?
  CASE
    WHEN COALESCE(curiosity_momentum, 0.5) > 0.65
     AND COALESCE(community_momentum, 0.5) > 0.65
      THEN 'demand_surging'
    WHEN COALESCE(curiosity_momentum, 0.5) > 0.55
      THEN 'demand_rising'
    WHEN COALESCE(curiosity_momentum, 0.5) < 0.35
      THEN 'demand_fading'
    ELSE 'demand_stable'
  END AS demand_trend,

  -- Confidence: how many data streams back this classification?
  streams_present,
  CASE
    WHEN streams_present >= 5 THEN 'HIGH'
    WHEN streams_present >= 3 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS classification_confidence,

  -- Overall context
  trend_level,
  trend_momentum,

  -- Rank within quadrant
  ROW_NUMBER() OVER (
    PARTITION BY CASE
      WHEN demand_score >= 0.6 AND supply_score < 0.4  THEN 'blue_ocean'
      WHEN demand_score >= 0.6 AND supply_score >= 0.4 THEN 'red_ocean'
      WHEN demand_score < 0.6  AND supply_score >= 0.4 THEN 'over_supplied'
      ELSE 'dead_zone'
    END
    ORDER BY GREATEST(COALESCE(demand_supply_gap, 0), 0) *
             COALESCE(demand_score, 0) *
             (1.0 + GREATEST(COALESCE(curiosity_momentum, 0.5) - 0.5, 0) * 2.0)
    DESC
  ) AS rank_in_quadrant,

  -- Rank within category (if categorized)
  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY GREATEST(COALESCE(demand_supply_gap, 0), 0) DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM classified;
