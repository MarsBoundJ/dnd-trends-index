-- Digital vs Tabletop Health Index: Is D&D's Center of Gravity Shifting?
-- Dataset: gold_data.composite_digital_vs_tabletop
-- Reads from: gold_data.composite_concept_index + analytics views
--
-- Macro narrative view: compares the health of digital D&D (BG3, Steam,
-- Twitch, mods, AO3) vs tabletop D&D (Roll20, BGG, Reddit, DMs Guild,
-- Fandom wikis). Shows whether the franchise's center of gravity is
-- shifting toward digital experiences.
--
-- Two levels of analysis:
--   1. Per-concept: does this concept live more in digital or tabletop?
--   2. Aggregate: across all concepts, which ecosystem is stronger?
--
-- Usage:
--   -- Concepts that are more digital than tabletop:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_digital_vs_tabletop`
--   WHERE ecosystem_home = 'digital_dominant' ORDER BY digital_score DESC
--
--   -- Concepts that bridge both worlds:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_digital_vs_tabletop`
--   WHERE ecosystem_home = 'balanced' ORDER BY total_ecosystem_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_digital_vs_tabletop` AS

WITH -- Digital ecosystem signals (Steam, Twitch, AO3, mods)
digital_signals AS (
  SELECT
    concept_name,
    stream_name,
    primary_metric,
    momentum,
    confidence,
    PERCENT_RANK() OVER (
      PARTITION BY stream_name ORDER BY primary_metric ASC
    ) AS norm_metric,
    PERCENT_RANK() OVER (
      PARTITION BY stream_name ORDER BY momentum ASC
    ) AS norm_momentum
  FROM `dnd-trends-index.gold_data.analytics_digital_gaming`
  WHERE primary_metric IS NOT NULL
),

digital_agg AS (
  SELECT
    concept_name,
    -- Confidence-weighted average across digital streams
    ROUND(AVG(norm_metric), 4) AS digital_score,
    ROUND(AVG(norm_momentum), 4) AS digital_momentum,
    COUNT(DISTINCT stream_name) AS digital_stream_count,
    STRING_AGG(DISTINCT stream_name, ', ' ORDER BY stream_name) AS digital_streams_present,
    -- Dominant digital platform
    MAX(CASE WHEN norm_metric = (SELECT MAX(norm_metric) FROM UNNEST([norm_metric]))
        THEN stream_name END) AS top_digital_stream
  FROM digital_signals
  GROUP BY concept_name
),

-- Tabletop ecosystem signals (Roll20, BGG, Reddit, DMs Guild, Fandom)
-- Pull from individual analytics views with normalization
tabletop_roll20 AS (
  SELECT concept_name, primary_metric, momentum,
    PERCENT_RANK() OVER (ORDER BY primary_metric ASC) AS norm_metric,
    PERCENT_RANK() OVER (ORDER BY momentum ASC) AS norm_momentum,
    'roll20' AS stream_name
  FROM `dnd-trends-index.gold_data.analytics_roll20`
  WHERE primary_metric IS NOT NULL
),
tabletop_bgg AS (
  SELECT concept_name, primary_metric, momentum,
    PERCENT_RANK() OVER (ORDER BY primary_metric ASC) AS norm_metric,
    PERCENT_RANK() OVER (ORDER BY momentum ASC) AS norm_momentum,
    'bgg_rpggeek' AS stream_name
  FROM `dnd-trends-index.gold_data.analytics_bgg`
  WHERE primary_metric IS NOT NULL
),
tabletop_reddit AS (
  SELECT concept_name, primary_metric, momentum,
    PERCENT_RANK() OVER (ORDER BY primary_metric ASC) AS norm_metric,
    PERCENT_RANK() OVER (ORDER BY momentum ASC) AS norm_momentum,
    'reddit' AS stream_name
  FROM `dnd-trends-index.gold_data.analytics_reddit`
  WHERE primary_metric IS NOT NULL
),
tabletop_dmsguild AS (
  SELECT concept_name, primary_metric, momentum,
    PERCENT_RANK() OVER (ORDER BY primary_metric ASC) AS norm_metric,
    PERCENT_RANK() OVER (ORDER BY momentum ASC) AS norm_momentum,
    'dmsguild' AS stream_name
  FROM `dnd-trends-index.gold_data.analytics_dmsguild_dtrpg`
  WHERE primary_metric IS NOT NULL AND stream_name = 'dmsguild'
),
tabletop_fandom AS (
  SELECT concept_name, primary_metric, momentum,
    PERCENT_RANK() OVER (ORDER BY primary_metric ASC) AS norm_metric,
    PERCENT_RANK() OVER (ORDER BY momentum ASC) AS norm_momentum,
    'fandom_wiki' AS stream_name
  FROM `dnd-trends-index.gold_data.analytics_fandom`
  WHERE primary_metric IS NOT NULL
),

-- Union all tabletop streams
tabletop_all AS (
  SELECT * FROM tabletop_roll20
  UNION ALL SELECT * FROM tabletop_bgg
  UNION ALL SELECT * FROM tabletop_reddit
  UNION ALL SELECT * FROM tabletop_dmsguild
  UNION ALL SELECT * FROM tabletop_fandom
),

tabletop_agg AS (
  SELECT
    concept_name,
    ROUND(AVG(norm_metric), 4) AS tabletop_score,
    ROUND(AVG(norm_momentum), 4) AS tabletop_momentum,
    COUNT(DISTINCT stream_name) AS tabletop_stream_count,
    STRING_AGG(DISTINCT stream_name, ', ' ORDER BY stream_name) AS tabletop_streams_present
  FROM tabletop_all
  GROUP BY concept_name
),

-- Combine both ecosystems
combined AS (
  SELECT
    COALESCE(d.concept_name, t.concept_name) AS concept_name,

    -- Digital ecosystem
    d.digital_score,
    d.digital_momentum,
    COALESCE(d.digital_stream_count, 0) AS digital_stream_count,
    d.digital_streams_present,

    -- Tabletop ecosystem
    t.tabletop_score,
    t.tabletop_momentum,
    COALESCE(t.tabletop_stream_count, 0) AS tabletop_stream_count,
    t.tabletop_streams_present,

    -- Presence flags
    CASE WHEN d.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_digital,
    CASE WHEN t.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_tabletop

  FROM digital_agg d
  FULL OUTER JOIN tabletop_agg t
    ON LOWER(d.concept_name) = LOWER(t.concept_name)
),

scored AS (
  SELECT
    c.*,

    -- Ecosystem balance: positive = more digital, negative = more tabletop
    ROUND(COALESCE(c.digital_score, 0) - COALESCE(c.tabletop_score, 0), 4)
      AS digital_tabletop_gap,

    -- Total ecosystem presence
    ROUND(COALESCE(c.digital_score, 0) + COALESCE(c.tabletop_score, 0), 4)
      AS total_ecosystem_score,

    -- Total streams
    c.digital_stream_count + c.tabletop_stream_count AS total_streams

  FROM combined c
)

SELECT
  s.concept_name,
  idx.category,
  idx.ruleset,

  -- Ecosystem scores (0-1)
  ROUND(COALESCE(s.digital_score, 0), 3) AS digital_score,
  ROUND(COALESCE(s.tabletop_score, 0), 3) AS tabletop_score,
  ROUND(s.digital_tabletop_gap, 3) AS digital_tabletop_gap,
  ROUND(s.total_ecosystem_score, 3) AS total_ecosystem_score,

  -- Ecosystem momentum
  ROUND(COALESCE(s.digital_momentum, 0), 3) AS digital_momentum,
  ROUND(COALESCE(s.tabletop_momentum, 0), 3) AS tabletop_momentum,

  -- Ecosystem home classification
  CASE
    -- DIGITAL DOMINANT: strong digital, weak/absent tabletop
    WHEN s.has_digital = TRUE AND (s.has_tabletop = FALSE OR s.tabletop_score < 0.3)
     AND COALESCE(s.digital_score, 0) >= 0.4
      THEN 'digital_dominant'

    -- TABLETOP DOMINANT: strong tabletop, weak/absent digital
    WHEN s.has_tabletop = TRUE AND (s.has_digital = FALSE OR s.digital_score < 0.3)
     AND COALESCE(s.tabletop_score, 0) >= 0.4
      THEN 'tabletop_dominant'

    -- BALANCED: significant presence in both
    WHEN s.has_digital = TRUE AND s.has_tabletop = TRUE
     AND ABS(COALESCE(s.digital_score, 0) - COALESCE(s.tabletop_score, 0)) < 0.3
      THEN 'balanced'

    -- DIGITAL LEANING: present in both but stronger digital
    WHEN s.has_digital = TRUE AND s.has_tabletop = TRUE
     AND COALESCE(s.digital_score, 0) > COALESCE(s.tabletop_score, 0)
      THEN 'digital_leaning'

    -- TABLETOP LEANING: present in both but stronger tabletop
    WHEN s.has_digital = TRUE AND s.has_tabletop = TRUE
     AND COALESCE(s.tabletop_score, 0) > COALESCE(s.digital_score, 0)
      THEN 'tabletop_leaning'

    -- DIGITAL ONLY: only appears in digital streams
    WHEN s.has_digital = TRUE AND s.has_tabletop = FALSE
      THEN 'digital_only'

    -- TABLETOP ONLY: only appears in tabletop streams
    WHEN s.has_tabletop = TRUE AND s.has_digital = FALSE
      THEN 'tabletop_only'

    ELSE 'minimal_signal'
  END AS ecosystem_home,

  -- Gravity shift: is this concept migrating between ecosystems?
  CASE
    WHEN COALESCE(s.digital_momentum, 0.5) > 0.6
     AND COALESCE(s.tabletop_momentum, 0.5) < 0.4
      THEN 'shifting_digital'
    WHEN COALESCE(s.tabletop_momentum, 0.5) > 0.6
     AND COALESCE(s.digital_momentum, 0.5) < 0.4
      THEN 'shifting_tabletop'
    WHEN COALESCE(s.digital_momentum, 0.5) > 0.6
     AND COALESCE(s.tabletop_momentum, 0.5) > 0.6
      THEN 'both_growing'
    WHEN COALESCE(s.digital_momentum, 0.5) < 0.4
     AND COALESCE(s.tabletop_momentum, 0.5) < 0.4
      THEN 'both_declining'
    ELSE 'stable'
  END AS gravity_shift,

  -- Stream detail
  s.digital_stream_count,
  s.tabletop_stream_count,
  s.total_streams,
  s.digital_streams_present,
  s.tabletop_streams_present,

  -- Context from composite index
  idx.trend_level,
  idx.demand_score,
  idx.supply_score,

  -- Confidence
  CASE
    WHEN s.total_streams >= 4 AND s.has_digital AND s.has_tabletop THEN 'HIGH'
    WHEN s.total_streams >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS confidence,

  -- Ranks
  ROW_NUMBER() OVER (ORDER BY s.digital_tabletop_gap DESC) AS rank_most_digital,
  ROW_NUMBER() OVER (ORDER BY s.digital_tabletop_gap ASC) AS rank_most_tabletop,
  ROW_NUMBER() OVER (ORDER BY s.total_ecosystem_score DESC) AS rank_total_health,

  CURRENT_DATE() AS snapshot_date

FROM scored s
LEFT JOIN `dnd-trends-index.gold_data.composite_concept_index` idx
  ON LOWER(s.concept_name) = LOWER(idx.concept_name);
