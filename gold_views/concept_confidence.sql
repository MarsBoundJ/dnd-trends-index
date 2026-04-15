-- Concept Confidence: Step 6 — per-concept data confidence score (0-99)
-- Dataset: gold_data.concept_confidence
-- Type: VIEW (reads from the materialized composite_concept_index)
--
-- Produces one row per concept with the data-layer confidence score
-- (the §5.1 "data confidence" number) plus the explanation payload
-- the methodology popover needs. AI grounding confidence is handled
-- separately at card-assembly time for AI-generated text; for pure
-- data cards the displayed score is data_confidence directly.
--
-- The score maps to the locked metal ladder from FRONTEND_DESIGN_SPEC §9.16:
--   copper    0-69    exploratory floor
--   silver    70-79   acceptable, rarely act
--   gold      80-89   decision-grade
--   platinum  90-94   highly trusted
--   mithral   95-99   gold-standard consensus
--
-- Formula (v1.0.0):
--   base = 30
--        + 25 * avg_stream_confidence_label        (HIGH=1.0, MED=0.7, LOW=0.4)
--        + 15 * streams_present / (streams_present + 2)
--        + 20 * families_hit / 5                   (curiosity/community/creator/ownership/commerce)
--        + 10 * agreement_factor                   (majority fraction of bucket momenta)
--   raw  = base * velocity_factor                  (stale-consensus penalty)
--   data_confidence = LEAST(raw, 79 if streams_present <= 1 else 99)
--
-- The single-stream hard cap (79 = silver ceiling) is the §5.3 anti-theater
-- rule expressed as a gate: a genuinely emerging concept surfaces honestly
-- without being upgraded to gold/platinum by high single-source conviction.
--
-- Velocity penalty (Gemini stale-consensus fix): concepts with high
-- trend_level but low trend_momentum ("5e is popular but not moving") get
-- trimmed so the evergreen giants don't permanently squat in mithral.
--
-- Known gap (v1.0.0): freshness_factor is hardcoded to 1.0. Stream-level
-- max(snapshot_date) is not yet surfaced through composite_concept_index;
-- when it is, multiply freshness_factor into raw without other changes.
--
-- Calibration: the weights above are first-draft. The anti-theater
-- calibration pass (Phase 4) will histogram the distribution by tier
-- and tune constants until the bulk lands in gold without any single
-- tier exceeding ~60% share. Tune the formula, never the ladder.
--
-- Usage:
--   SELECT concept_name, data_confidence, tier, why_not_higher
--   FROM `dnd-trends-index.gold_data.concept_confidence`
--   WHERE concept_name = 'Paladin'

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.concept_confidence` AS

WITH factors AS (
  SELECT
    concept_name,
    category,
    streams_present,

    -- families_hit: count of the 5 families (curiosity/community/creator/
    -- ownership/commerce) with at least one contributing stream
    (
      CASE WHEN curiosity_streams > 0 THEN 1 ELSE 0 END +
      CASE WHEN community_streams > 0 THEN 1 ELSE 0 END +
      CASE WHEN creator_streams   > 0 THEN 1 ELSE 0 END +
      CASE WHEN ownership_streams > 0 THEN 1 ELSE 0 END +
      CASE WHEN commerce_streams  > 0 THEN 1 ELSE 0 END
    ) AS families_hit,

    -- avg stream confidence weight across buckets that have data
    -- (each bucket's avg_confidence is itself an average of its streams'
    -- HIGH=1.0/MED=0.7/LOW=0.4 weights). NULL-safe via UNNEST + WHERE.
    (
      SELECT AVG(c) FROM UNNEST([
        curiosity_avg_confidence,
        community_avg_confidence,
        creator_avg_confidence,
        ownership_avg_confidence,
        commerce_avg_confidence
      ]) AS c WHERE c IS NOT NULL
    ) AS concept_avg_confidence,

    -- agreement: across non-null bucket momenta, what fraction are on
    -- the majority side of 0.5? Values near 0.5 (neutral) fall into
    -- neither camp, so genuinely neutral concepts have agreement = 1
    -- only if all buckets are neutral together.
    (
      SELECT
        SAFE_DIVIDE(
          GREATEST(COUNTIF(m > 0.5), COUNTIF(m < 0.5)),
          NULLIF(COUNT(m), 0)
        )
      FROM UNNEST([
        curiosity_momentum,
        community_momentum,
        creator_momentum,
        ownership_momentum,
        commerce_momentum
      ]) AS m
      WHERE m IS NOT NULL
    ) AS agreement_factor,

    -- Stale-consensus velocity penalty: high absolute signal,
    -- low directional movement → evergreen giant, trim the score.
    CASE
      WHEN trend_level >= 0.7 AND trend_momentum < 0.4 THEN 0.90
      WHEN trend_level >= 0.5 AND trend_momentum < 0.3 THEN 0.95
      ELSE 1.0
    END AS velocity_factor,

    -- Freshness: placeholder until stream-level max snapshot_date flows
    -- through composite_concept_index. See "Known gap" in header.
    1.0 AS freshness_factor,

    trend_level,
    trend_momentum,
    snapshot_date
  FROM `dnd-trends-index.gold_data.composite_concept_index`
),

scored AS (
  SELECT
    *,
    -- Base score assembly (each term caps at the constant shown)
    30.0
      + 25.0 * COALESCE(concept_avg_confidence, 0.4)
      + 15.0 * SAFE_DIVIDE(streams_present, streams_present + 2)
      + 20.0 * SAFE_DIVIDE(families_hit, 5)
      + 10.0 * COALESCE(agreement_factor, 0.5)
      AS base_score
  FROM factors
),

raw AS (
  SELECT
    *,
    base_score * velocity_factor * freshness_factor AS raw_score
  FROM scored
),

capped AS (
  SELECT
    *,
    -- Single-stream hard cap → silver ceiling (79)
    -- Everyone else is capped at 99 (mithral ceiling)
    CAST(ROUND(
      CASE
        WHEN streams_present <= 1 THEN LEAST(raw_score, 79.0)
        ELSE LEAST(raw_score, 99.0)
      END
    ) AS INT64) AS data_confidence,
    CASE
      WHEN streams_present <= 1 AND raw_score > 79.0 THEN TRUE
      ELSE FALSE
    END AS sparsity_cap_applied
  FROM raw
)

SELECT
  concept_name,
  category,
  data_confidence,

  -- Metal-ladder tier, kept in sync with confidenceToTier() in the frontend.
  -- The frontend function is the user-facing source of truth; this column
  -- exists so BQ-side consumers (Sage tools, debugging) don't have to
  -- re-implement the ladder.
  CASE
    WHEN data_confidence >= 95 THEN 'mithral'
    WHEN data_confidence >= 90 THEN 'platinum'
    WHEN data_confidence >= 80 THEN 'gold'
    WHEN data_confidence >= 70 THEN 'silver'
    ELSE 'copper'
  END AS tier,

  -- ---------------------------------------------------------------
  -- Factor breakdown (for the methodology popover)
  -- ---------------------------------------------------------------
  streams_present,
  families_hit,
  ROUND(COALESCE(concept_avg_confidence, 0.4), 3) AS avg_stream_confidence,
  ROUND(COALESCE(agreement_factor, 0.5), 3)       AS agreement_factor,
  ROUND(velocity_factor, 3)                       AS velocity_factor,
  ROUND(freshness_factor, 3)                      AS freshness_factor,
  sparsity_cap_applied,

  -- Binding constraint: whichever factor is pulling the score down the
  -- most. Ordered by which constraint "matters most" to a user reading
  -- the popover, not by pure arithmetic contribution — e.g. single_source
  -- always wins over limited_diversity because it's the more fundamental
  -- data-thinness signal. Calibration may reorder these during Phase 4.
  CASE
    WHEN streams_present <= 1            THEN 'single_source'
    WHEN families_hit <= 2               THEN 'limited_diversity'
    WHEN agreement_factor < 0.7          THEN 'conflicting_signals'
    WHEN velocity_factor < 1.0           THEN 'stale_consensus'
    WHEN concept_avg_confidence < 0.7    THEN 'thin_stream_data'
    ELSE 'strong'
  END AS binding_constraint,

  -- Human-readable one-liner. Deterministic — no LLM call.
  CASE
    WHEN streams_present <= 1 THEN
      'Only one data stream tracks this concept. Cross-stream corroboration would lift the score.'
    WHEN families_hit <= 2 THEN
      'Signal is present but concentrated in few evidence families. Broader coverage would lift the score.'
    WHEN agreement_factor < 0.7 THEN
      'Different data families point in different directions. Agreement across families would lift the score.'
    WHEN velocity_factor < 1.0 THEN
      'High absolute volume with little recent movement — evergreen signal, not active momentum.'
    WHEN concept_avg_confidence < 0.7 THEN
      'Data is present across streams but the underlying data points are thin.'
    ELSE
      'Strong cross-validated evidence across families.'
  END AS why_not_higher,

  -- The full explanation object. Bouncer passes this through to the
  -- frontend methodology popover; the Sage getConfidenceExplanation tool
  -- (Step 7) reads the same payload.
  TO_JSON(STRUCT(
    data_confidence                         AS data,
    100                                     AS ai_grounding,  -- overridden at card-assembly time for AI cards
    data_confidence                         AS displayed,
    'v1.0.0'                                AS algo_version,
    streams_present                         AS streams_present,
    families_hit                            AS families_hit,
    ROUND(COALESCE(concept_avg_confidence, 0.4), 3) AS avg_stream_confidence,
    ROUND(COALESCE(agreement_factor, 0.5), 3)       AS agreement,
    ROUND(velocity_factor, 3)               AS velocity_factor,
    ROUND(freshness_factor, 3)              AS freshness_factor,
    sparsity_cap_applied                    AS sparsity_cap_applied,
    CASE
      WHEN streams_present <= 1            THEN 'single_source'
      WHEN families_hit <= 2               THEN 'limited_diversity'
      WHEN agreement_factor < 0.7          THEN 'conflicting_signals'
      WHEN velocity_factor < 1.0           THEN 'stale_consensus'
      WHEN concept_avg_confidence < 0.7    THEN 'thin_stream_data'
      ELSE 'strong'
    END AS binding_constraint
  )) AS explanation_json,

  'v1.0.0'       AS algo_version,
  snapshot_date
FROM capped
WHERE data_confidence IS NOT NULL;
