-- Edition-Aware vs Edition-Agnostic: 2024 Edition Adoption Tracker
-- Dataset: gold_data.composite_nostalgia_novelty
-- Reads from: expanded_search_terms + trend_data_pilot + concept_library +
--             composite_concept_index
--
-- Exploits the search term expansion system: for each concept like "Ranger",
-- we track separate Google Trends scores for:
--   "Ranger 2024"  = EDITION-AWARE — definitively targets new edition (clean signal)
--   "Ranger 5.5e"  = EDITION-AWARE — definitively targets new edition (rising as WotC adopts name)
--   "Ranger dnd"   = EDITION-AGNOSTIC — user wants D&D info, doesn't specify edition
--   "Ranger 5e"    = LEGACY-AWARE — definitively targets 2014 edition (near zero)
--
-- Key insight: "dnd" suffix is AMBIGUOUS — it could mean 2014 or 2024.
-- It represents efficient/lazy searching by users who don't specify an edition.
-- Therefore we treat it as the denominator (total pool), not as a legacy signal.
--
-- ADOPTION RATE = (2024 + 5.5e) / (2024 + 5.5e + dnd)
-- "Of all D&D searches for this concept, what fraction are edition-aware (new edition)?"
--
-- WotC question: "How fast is each concept becoming edition-aware under the 2024 rules?"
--
-- Usage:
--   -- Most edition-aware concepts:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_nostalgia_novelty`
--   WHERE adoption_stage = 'edition_aware' ORDER BY adoption_rate DESC
--
--   -- Edition awareness by category:
--   SELECT category, ROUND(AVG(adoption_rate),3) as avg_adoption
--   FROM `dnd-trends-index.gold_data.composite_nostalgia_novelty`
--   WHERE confidence != 'LOW'
--   GROUP BY category ORDER BY avg_adoption DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_nostalgia_novelty` AS

WITH -- Step 1: Get expanded search terms that have edition-specific suffixes
edition_terms AS (
  SELECT
    e.original_keyword AS base_concept,
    e.search_term,
    e.category,
    e.term_id,
    -- Classify the suffix
    CASE
      WHEN LOWER(e.search_term) LIKE '%2024%' THEN '2024'
      WHEN LOWER(e.search_term) LIKE '%5.5%' OR LOWER(e.search_term) LIKE '%5.5e%' THEN '5.5e'
      WHEN LOWER(e.search_term) LIKE '%dnd%' OR LOWER(e.search_term) LIKE '%d&d%' THEN 'generic_dnd'
      WHEN LOWER(e.search_term) LIKE '%5e%' AND LOWER(e.search_term) NOT LIKE '%5.5e%' THEN '5e_legacy'
      WHEN LOWER(e.search_term) LIKE '%build%' THEN 'build'
      ELSE 'other'
    END AS suffix_class
  FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` e
  WHERE e.is_official = TRUE OR e.is_pilot = TRUE
),

-- Step 2: Join to trend data for scores (last 14 days)
term_scores AS (
  SELECT
    et.base_concept,
    et.search_term,
    et.category,
    et.suffix_class,
    AVG(t.interest) AS avg_interest_14d,
    MAX(t.interest) AS max_interest_14d,
    COUNT(DISTINCT t.date) AS data_points
  FROM edition_terms et
  JOIN `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    ON et.term_id = t.term_id
  WHERE t.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    AND et.suffix_class IN ('2024', '5.5e', 'generic_dnd', '5e_legacy')
  GROUP BY et.base_concept, et.search_term, et.category, et.suffix_class
),

-- Step 3: Also get 30d-ago scores for momentum
term_scores_prev AS (
  SELECT
    et.base_concept,
    et.suffix_class,
    AVG(t.interest) AS avg_interest_prev14d
  FROM edition_terms et
  JOIN `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    ON et.term_id = t.term_id
  WHERE t.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    AND t.date < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    AND et.suffix_class IN ('2024', '5.5e', 'generic_dnd', '5e_legacy')
  GROUP BY et.base_concept, et.suffix_class
),

-- Step 4: Aggregate per concept per suffix class
concept_edition AS (
  SELECT
    ts.base_concept,
    ts.category,
    ts.suffix_class,
    MAX(ts.avg_interest_14d) AS interest_14d,
    MAX(ts.max_interest_14d) AS peak_14d,
    SUM(ts.data_points) AS total_data_points,
    MAX(tsp.avg_interest_prev14d) AS interest_prev14d
  FROM term_scores ts
  LEFT JOIN term_scores_prev tsp
    ON ts.base_concept = tsp.base_concept
    AND ts.suffix_class = tsp.suffix_class
  GROUP BY ts.base_concept, ts.category, ts.suffix_class
),

-- Step 5: Pivot edition classes into columns per concept
pivoted AS (
  SELECT
    base_concept,
    category,

    -- 2024 edition interest (CLEAN new-edition signal)
    MAX(CASE WHEN suffix_class = '2024' THEN interest_14d END) AS interest_2024,
    MAX(CASE WHEN suffix_class = '2024' THEN peak_14d END) AS peak_2024,
    MAX(CASE WHEN suffix_class = '2024' THEN interest_prev14d END) AS prev_2024,

    -- 5.5e interest (CLEAN new-edition signal, expected to grow)
    MAX(CASE WHEN suffix_class = '5.5e' THEN interest_14d END) AS interest_5_5e,
    MAX(CASE WHEN suffix_class = '5.5e' THEN peak_14d END) AS peak_5_5e,
    MAX(CASE WHEN suffix_class = '5.5e' THEN interest_prev14d END) AS prev_5_5e,

    -- Generic "dnd" interest (AMBIGUOUS — could be either edition)
    MAX(CASE WHEN suffix_class = 'generic_dnd' THEN interest_14d END) AS interest_generic,
    MAX(CASE WHEN suffix_class = 'generic_dnd' THEN peak_14d END) AS peak_generic,
    MAX(CASE WHEN suffix_class = 'generic_dnd' THEN interest_prev14d END) AS prev_generic,

    -- Legacy 5e interest (CLEAN legacy signal — near zero for most concepts)
    MAX(CASE WHEN suffix_class = '5e_legacy' THEN interest_14d END) AS interest_5e,
    MAX(CASE WHEN suffix_class = '5e_legacy' THEN peak_14d END) AS peak_5e,
    MAX(CASE WHEN suffix_class = '5e_legacy' THEN interest_prev14d END) AS prev_5e

  FROM concept_edition
  GROUP BY base_concept, category
),

-- Step 6: Compute edition adoption metrics
analyzed AS (
  SELECT
    p.base_concept AS concept_name,
    p.category,

    -- Raw interest values (labeled by signal clarity)
    ROUND(COALESCE(p.interest_2024, 0), 1) AS interest_2024,
    ROUND(COALESCE(p.interest_5_5e, 0), 1) AS interest_5_5e,
    ROUND(COALESCE(p.interest_generic, 0), 1) AS interest_agnostic,
    ROUND(COALESCE(p.interest_5e, 0), 1) AS interest_legacy,

    -- Edition-specific interest (clean signal: definitively new edition)
    ROUND(COALESCE(p.interest_2024, 0) + COALESCE(p.interest_5_5e, 0), 1)
      AS edition_specific_interest,

    -- Edition-agnostic interest (ambiguous: could be either edition)
    ROUND(COALESCE(p.interest_generic, 0), 1)
      AS edition_agnostic_interest,

    -- Legacy-specific interest (clean signal: definitively old edition)
    ROUND(COALESCE(p.interest_5e, 0), 1)
      AS legacy_specific_interest,

    -- =====================================================
    -- ADOPTION RATE: what fraction of D&D searches specify the new edition?
    -- = (2024 + 5.5e) / (2024 + 5.5e + dnd)
    -- Excludes 5e_legacy from denominator (it's a separate dying signal)
    -- =====================================================
    ROUND(SAFE_DIVIDE(
      COALESCE(p.interest_2024, 0) + COALESCE(p.interest_5_5e, 0),
      GREATEST(
        COALESCE(p.interest_2024, 0) + COALESCE(p.interest_5_5e, 0) +
        COALESCE(p.interest_generic, 0),
        1
      )
    ), 4) AS adoption_rate,

    -- Adoption momentum: is the adoption rate growing?
    -- Compare current new-edition interest vs previous
    ROUND(SAFE_DIVIDE(
      COALESCE(p.interest_2024, 0) - COALESCE(p.prev_2024, 0),
      GREATEST(COALESCE(p.prev_2024, 0), 1)
    ), 3) AS adoption_momentum_2024,

    -- 5.5e momentum (expected to rise post-WotC announcement)
    ROUND(SAFE_DIVIDE(
      COALESCE(p.interest_5_5e, 0) - COALESCE(p.prev_5_5e, 0),
      GREATEST(COALESCE(p.prev_5_5e, 0), 1)
    ), 3) AS adoption_momentum_5_5e,

    -- Generic "dnd" momentum (is the ambiguous pool shrinking as people specify edition?)
    ROUND(SAFE_DIVIDE(
      COALESCE(p.interest_generic, 0) - COALESCE(p.prev_generic, 0),
      GREATEST(COALESCE(p.prev_generic, 0), 1)
    ), 3) AS agnostic_momentum,

    -- Legacy signal momentum (expected to stay near zero)
    ROUND(SAFE_DIVIDE(
      COALESCE(p.interest_5e, 0) - COALESCE(p.prev_5e, 0),
      GREATEST(COALESCE(p.prev_5e, 0), 1)
    ), 3) AS legacy_momentum,

    -- Total interest across all variants (concept heat regardless of edition)
    ROUND(
      COALESCE(p.interest_2024, 0) + COALESCE(p.interest_5_5e, 0) +
      COALESCE(p.interest_generic, 0) + COALESCE(p.interest_5e, 0)
    , 1) AS total_interest,

    -- Data completeness
    (CASE WHEN p.interest_2024 IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.interest_5_5e IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.interest_generic IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.interest_5e IS NOT NULL THEN 1 ELSE 0 END) AS variant_count

  FROM pivoted p
  -- Require at least some interest
  WHERE COALESCE(p.interest_2024, 0) + COALESCE(p.interest_5_5e, 0) +
        COALESCE(p.interest_generic, 0) + COALESCE(p.interest_5e, 0) > 0
)

SELECT
  a.concept_name,
  a.category,
  idx.ruleset,

  -- =====================================================
  -- ADOPTION RATE & STAGE (edition-aware vs edition-agnostic)
  -- =====================================================
  a.adoption_rate,
  CASE
    -- EDITION-AWARE: >60% of searches specify new edition
    WHEN a.adoption_rate >= 0.6 THEN 'edition_aware'
    -- BECOMING AWARE: 30-60% specify new edition, rest are agnostic
    WHEN a.adoption_rate >= 0.3 THEN 'becoming_aware'
    -- MOSTLY AGNOSTIC: some new-edition searches but <30%
    WHEN a.adoption_rate > 0
     AND (a.interest_2024 > 0 OR a.interest_5_5e > 0)
      THEN 'mostly_agnostic'
    -- FULLY AGNOSTIC: no new-edition searches, only generic "dnd"
    WHEN a.interest_agnostic > 0
     AND a.edition_specific_interest = 0
      THEN 'fully_agnostic'
    ELSE 'insufficient_data'
  END AS adoption_stage,

  -- =====================================================
  -- ADOPTION TRAJECTORY: is adoption accelerating?
  -- =====================================================
  CASE
    -- ACCELERATING: new-edition interest rising AND agnostic pool shrinking
    -- (people switching from "Ranger dnd" to "Ranger 2024")
    WHEN a.adoption_momentum_2024 > 0.1 AND a.agnostic_momentum < 0
      THEN 'accelerating'
    -- GROWING: new-edition interest rising, agnostic pool stable
    -- (new searches are additive, not cannibalizing generic)
    WHEN a.adoption_momentum_2024 > 0.1 AND a.agnostic_momentum >= 0
      THEN 'growing'
    -- 5.5e EMERGING: 5.5e term rising (post-WotC naming announcement)
    WHEN a.adoption_momentum_5_5e > 0.2
      THEN 'naming_shift'
    -- PLATEAU: adoption rate stable
    WHEN ABS(COALESCE(a.adoption_momentum_2024, 0)) <= 0.1
     AND a.adoption_rate > 0
      THEN 'plateau'
    -- NOT STARTED: no new-edition signal at all
    WHEN a.edition_specific_interest = 0
      THEN 'not_started'
    -- DECLINING: new-edition interest falling
    WHEN a.adoption_momentum_2024 < -0.1
      THEN 'cooling'
    ELSE 'stable'
  END AS adoption_trajectory,

  -- =====================================================
  -- INTEREST BREAKDOWN (labeled by signal clarity)
  -- =====================================================

  -- Clean new-edition signals
  a.interest_2024,
  a.interest_5_5e,
  a.edition_specific_interest,    -- combined clean new-edition signal

  -- Ambiguous signal
  a.interest_agnostic,            -- "dnd" suffix — could be either edition

  -- Clean legacy signal
  a.interest_legacy,              -- "5e" suffix — definitively old edition
  a.legacy_specific_interest,

  -- Overall concept heat
  a.total_interest,

  -- =====================================================
  -- MOMENTUM (per suffix)
  -- =====================================================
  a.adoption_momentum_2024,       -- is "2024" suffix growing?
  a.adoption_momentum_5_5e,       -- is "5.5e" suffix growing? (watch this one)
  a.agnostic_momentum,            -- is "dnd" suffix shrinking? (good sign for adoption)
  a.legacy_momentum,              -- is "5e" suffix dying? (expected)

  -- =====================================================
  -- VARIANT COVERAGE & CONFIDENCE
  -- =====================================================
  a.variant_count,

  CASE
    WHEN a.variant_count >= 3 AND a.total_interest >= 10 THEN 'HIGH'
    WHEN a.variant_count >= 2 AND a.total_interest >= 5 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS confidence,

  -- =====================================================
  -- CONTEXT from composite index
  -- =====================================================
  idx.demand_score,
  idx.community_score,
  idx.trend_level,

  -- =====================================================
  -- RANKS
  -- =====================================================
  ROW_NUMBER() OVER (ORDER BY a.total_interest DESC) AS rank_by_interest,
  ROW_NUMBER() OVER (ORDER BY a.adoption_rate DESC) AS rank_by_adoption,
  ROW_NUMBER() OVER (
    PARTITION BY a.category
    ORDER BY a.adoption_rate DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM analyzed a
LEFT JOIN `dnd-trends-index.gold_data.composite_concept_index` idx
  ON LOWER(a.concept_name) = LOWER(idx.concept_name);
