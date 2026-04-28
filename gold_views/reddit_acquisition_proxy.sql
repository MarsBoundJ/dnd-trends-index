-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.reddit_acquisition_proxy — Stage 5 Phase 2 of UB Matrix
-- ═══════════════════════════════════════════════════════════════════════
--
-- A NEW UB Matrix dimension: reverse-funnel customer-acquisition signal.
-- Per IP: do that IP's fans want D&D content / a D&D crossover?
--
-- This is ORTHOGONAL to community_reception (which measures whether
-- existing D&D players would accept the IP). community_reception asks
-- "would established D&D customers accept this?" Acquisition asks
-- "would this IP's fans become NEW D&D customers?"
--
-- The two answers can diverge sharply, and the divergence IS the
-- demo-grade pitch line:
--
--   "Pokemon: low D&D-side reception, but massive untapped acquisition
--    — r/Pokemon discusses D&D 4× more often than r/DnD discusses
--    Pokemon. Customer acquisition cost would be very low."
--
-- Per locked decision #1 (sibling scores never collapsed): this score
-- is NOT averaged into community_reception_score. It surfaces as a
-- separate matrix column.
--
-- ─── DATA FLOW ──────────────────────────────────────────────────────────
--
-- harvest_reddit_reverse_funnel.py
--   Searches each IP's primary subreddit (from ub_ip_alias_library)
--   for D&D-related terms over the last 30 days.
--          ↓
-- dnd_trends_raw.reddit_reverse_funnel_candidates
--          ↓
-- classify_reddit_reverse_funnel.py
--   Gemini Flash AI Bouncer. Per (post, ip) classifies:
--     is_about_dnd_crossover (binary disambiguation)
--     crossover_demand: wants_crossover | has_dnd_inspired |
--                        mentions_only | against_crossover |
--                        not_about_dnd
--          ↓
-- dnd_trends_raw.reddit_reverse_funnel_classified
--          ↓
-- gold_data.reddit_acquisition_proxy   (THIS VIEW)
--
-- ─── SCORE FORMULA ──────────────────────────────────────────────────────
--
-- Per mention, demand_score:
--   wants_crossover     = +1.0    (explicit demand for D&D content)
--   has_dnd_inspired    = +0.7    (organic creation — already half-converted)
--   mentions_only       =  0.0    (D&D mentioned, no demand signal)
--   against_crossover   = -1.0    (explicit rejection)
--   not_about_dnd       excluded
--
-- Per IP, weighted average:
--   demand_avg = SUM(demand_score × confidence) / SUM(confidence)
--   reddit_acquisition_score = (demand_avg + 1) / 2     [maps -1..1 -> 0..1]
--
-- Volume floor: <5 confirmed mentions → NULL with abstention.
--
-- ─── CONFIDENCE ─────────────────────────────────────────────────────────
--
--   <5  confirmed mentions  →  acquisition_score = NULL,
--                               status = 'insufficient_data'
--   5-15 confirmed           →  confidence = LOW
--   15-50 confirmed          →  confidence = MEDIUM
--   50+ confirmed            →  confidence = HIGH
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.reddit_acquisition_proxy` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Filter to confirmed-relevant mentions only.
  -- ────────────────────────────────────────────────────────────────────
  confirmed AS (
    SELECT
      ip_name,
      post_id,
      subreddit,
      crossover_demand,
      confidence,
      CASE crossover_demand
        WHEN 'wants_crossover'   THEN  1.0
        WHEN 'has_dnd_inspired'  THEN  0.7
        WHEN 'mentions_only'     THEN  0.0
        WHEN 'against_crossover' THEN -1.0
        ELSE NULL
      END AS demand_score
    FROM `dnd-trends-index.dnd_trends_raw.reddit_reverse_funnel_classified`
    WHERE is_about_dnd_crossover = TRUE
      AND crossover_demand != 'not_about_dnd'
  ),

  per_ip AS (
    SELECT
      ip_name,
      COUNT(*) AS confirmed_mentions,
      COUNT(DISTINCT subreddit) AS subreddit_spread,
      COUNTIF(crossover_demand = 'wants_crossover') AS wants_count,
      COUNTIF(crossover_demand = 'has_dnd_inspired') AS dnd_inspired_count,
      COUNTIF(crossover_demand = 'mentions_only') AS mentions_only_count,
      COUNTIF(crossover_demand = 'against_crossover') AS against_count,
      SAFE_DIVIDE(
        SUM(demand_score * confidence),
        NULLIF(SUM(confidence), 0)
      ) AS demand_avg
    FROM confirmed
    GROUP BY ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      a.primary_subreddit,
      p.confirmed_mentions,
      p.subreddit_spread,
      p.wants_count,
      p.dnd_inspired_count,
      p.mentions_only_count,
      p.against_count,
      p.demand_avg
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN `dnd-trends-index.dnd_trends_raw.ub_ip_alias_library` a
      ON a.ip_name = s.ip_name
    LEFT JOIN per_ip p ON p.ip_name = s.ip_name
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,
  j.primary_subreddit AS ip_subreddit,

  -- The score
  CASE
    WHEN COALESCE(j.confirmed_mentions, 0) < 5 THEN NULL
    ELSE ROUND((j.demand_avg + 1.0) / 2.0, 4)
  END AS reddit_acquisition_score,

  -- Status + confidence
  CASE
    WHEN j.primary_subreddit IS NULL OR j.primary_subreddit = '' THEN 'no_subreddit'
    WHEN COALESCE(j.confirmed_mentions, 0) = 0 THEN 'no_dnd_mentions'
    WHEN j.confirmed_mentions < 5 THEN 'insufficient_data'
    ELSE 'sufficient'
  END AS acquisition_status,

  CASE
    WHEN COALESCE(j.confirmed_mentions, 0) < 5 THEN 'NONE'
    WHEN j.confirmed_mentions < 15 THEN 'LOW'
    WHEN j.confirmed_mentions < 50 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS acquisition_signal_confidence,

  -- Volume + breakdown
  COALESCE(j.confirmed_mentions, 0) AS confirmed_mentions,
  COALESCE(j.subreddit_spread, 0) AS subreddit_spread,
  COALESCE(j.wants_count, 0) AS wants_count,
  COALESCE(j.dnd_inspired_count, 0) AS dnd_inspired_count,
  COALESCE(j.mentions_only_count, 0) AS mentions_only_count,
  COALESCE(j.against_count, 0) AS against_count,

  -- Reasoning string for the data trail
  CASE
    WHEN j.primary_subreddit IS NULL OR j.primary_subreddit = '' THEN
      'No primary subreddit known for this IP — reverse-funnel cannot scan.'
    WHEN COALESCE(j.confirmed_mentions, 0) = 0 THEN
      CONCAT('No D&D-crossover discussion detected in r/',
             j.primary_subreddit, ' over the last 30 days.')
    WHEN j.confirmed_mentions < 5 THEN
      CONCAT('Only ', CAST(j.confirmed_mentions AS STRING),
             ' confirmed mentions in r/', j.primary_subreddit,
             ' — too thin to score reliably.')
    ELSE
      CONCAT(
        CAST(j.confirmed_mentions AS STRING),
        ' confirmed D&D-crossover signals in r/', j.primary_subreddit,
        '. Breakdown: ',
        CAST(j.wants_count AS STRING), ' want-crossover / ',
        CAST(j.dnd_inspired_count AS STRING), ' D&D-inspired / ',
        CAST(j.mentions_only_count AS STRING), ' mentions-only / ',
        CAST(j.against_count AS STRING), ' against. ',
        'Demand average: ',
        CAST(ROUND(j.demand_avg, 2) AS STRING), '.'
      )
  END AS acquisition_reasoning,

  'acquisition_opportunity' AS signal_type,
  'reddit_ip_subreddit_demand' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM joined j
ORDER BY reddit_acquisition_score DESC NULLS LAST;
