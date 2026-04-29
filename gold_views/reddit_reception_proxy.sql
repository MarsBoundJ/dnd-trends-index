-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.reddit_reception_proxy — Stage 5 of community_reception
-- ═══════════════════════════════════════════════════════════════════════
--
-- For each UB candidate IP, aggregates AI-Bouncer-classified Reddit
-- mentions into a per-IP reddit_proxy_score in [0, 1].
--
-- The premise: D&D-subreddit discourse about an IP — and especially
-- the crossover_attitude classifier (positive / negative / divisive /
-- mentions_only) — measures how the established D&D community would
-- receive the IP as official D&D content. This is the "live discourse"
-- complement to the other 4 community_reception sources (Gemini
-- baseline + MTG/D&D precedents + BGG + AO3 fanfic).
--
-- ─── DATA FLOW ──────────────────────────────────────────────────────────
--
-- harvest_reddit_ub_candidates.py
--   Searches the top 7 D&D subreddits for posts mentioning each IP.
--   Front-line filters: co-term gating (for ambiguous IPs) +
--   banned-context exclusion. High recall, low precision.
--          ↓
-- dnd_trends_raw.reddit_ub_candidate_posts
--   Raw candidate posts (id, title, selftext, subreddit, score,
--   created_utc, etc.) — many are coincidental keyword matches.
--          ↓
-- classify_reddit_ub_mentions.py
--   Gemini Flash AI Bouncer: per (post, ip) pair, classifies
--   is_about_ip (binary), ip_affinity (-1..1), crossover_attitude
--   (positive/negative/divisive/mentions_only/not_about_ip),
--   confidence (0..1).
--          ↓
-- dnd_trends_raw.reddit_ub_classified_mentions
--          ↓
-- gold_data.reddit_reception_proxy   (THIS VIEW)
--          ↓
-- (consumed by future composite community_reception view)
--
-- ─── SCORE FORMULA ──────────────────────────────────────────────────────
--
-- reddit_proxy_score lives on the same [0.0, 1.0] scale as the other
-- community_reception sub-scores. High = better received.
--
-- Per mention, attitude_score:
--   positive       = +1.0
--   mentions_only  =  0.0  (neutral; no fit-evaluation)
--   divisive       = -0.4  (mild negative — split community)
--   negative       = -1.0
--   not_about_ip   excluded
--
-- Per IP, weighted average:
--   attitude_avg = SUM(attitude_score × confidence)
--                / SUM(confidence)
--   reddit_proxy_score = (attitude_avg + 1) / 2     [maps -1..1 -> 0..1]
--
-- ─── ABSTENTION ─────────────────────────────────────────────────────────
--
-- Some IPs will have very few confirmed mentions (e.g. obscure indie
-- video games, niche manhwa). Forcing a score on thin data fakes
-- precision. Instead:
--
--   < 5  confirmed mentions  →  reddit_proxy_score = NULL,
--                               status = 'insufficient_data'
--   5-15 confirmed           →  score with confidence = LOW
--   15-50 confirmed          →  confidence = MEDIUM
--   50+ confirmed            →  confidence = HIGH
--
-- The eventual composite community_reception_score handles NULL via
-- per-IP renormalization across present sources (same pattern as
-- PR #65). An IP with no Reddit signal still gets a valid composite
-- from the other 4 sources.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.reddit_reception_proxy` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Filter to confirmed-relevant mentions only.
  -- ────────────────────────────────────────────────────────────────────
  confirmed AS (
    SELECT
      ip_name,
      post_id,
      subreddit,
      ip_affinity,
      crossover_attitude,
      confidence,
      CASE crossover_attitude
        WHEN 'positive'      THEN  1.0
        WHEN 'mentions_only' THEN  0.0
        WHEN 'divisive'      THEN -0.4
        WHEN 'negative'      THEN -1.0
        ELSE NULL
      END AS attitude_score
    FROM `dnd-trends-index.dnd_trends_raw.reddit_ub_classified_mentions`
    WHERE is_about_ip = TRUE
      AND crossover_attitude != 'not_about_ip'
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-IP aggregation.
  -- ────────────────────────────────────────────────────────────────────
  per_ip AS (
    SELECT
      ip_name,
      COUNT(*) AS confirmed_mentions,
      COUNT(DISTINCT subreddit) AS subreddit_spread,
      COUNTIF(crossover_attitude = 'positive') AS positive_count,
      COUNTIF(crossover_attitude = 'mentions_only') AS mentions_only_count,
      COUNTIF(crossover_attitude = 'divisive') AS divisive_count,
      COUNTIF(crossover_attitude = 'negative') AS negative_count,
      ROUND(AVG(ip_affinity), 3) AS avg_ip_affinity,
      -- Weighted attitude average; confidence-weighted so high-
      -- confidence judgments dominate
      SAFE_DIVIDE(
        SUM(attitude_score * confidence),
        NULLIF(SUM(confidence), 0)
      ) AS attitude_avg
    FROM confirmed
    GROUP BY ip_name
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Top subreddit per IP (most mentions). Useful for the data trail.
  -- ────────────────────────────────────────────────────────────────────
  top_sub_per_ip AS (
    SELECT ip_name, subreddit AS top_subreddit, n
    FROM (
      SELECT ip_name, subreddit, COUNT(*) AS n,
        ROW_NUMBER() OVER (PARTITION BY ip_name ORDER BY COUNT(*) DESC) AS rn
      FROM confirmed
      GROUP BY ip_name, subreddit
    )
    WHERE rn = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Join seed list LEFT against confirmed data so unmapped/no-mention
  -- IPs surface as NULL rows (rather than dropping out).
  -- ────────────────────────────────────────────────────────────────────
  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      p.confirmed_mentions,
      p.subreddit_spread,
      p.positive_count,
      p.mentions_only_count,
      p.divisive_count,
      p.negative_count,
      p.avg_ip_affinity,
      p.attitude_avg,
      ts.top_subreddit
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip p ON p.ip_name = s.ip_name
    LEFT JOIN top_sub_per_ip ts ON ts.ip_name = s.ip_name
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- The score
  CASE
    WHEN COALESCE(j.confirmed_mentions, 0) < 5 THEN NULL
    ELSE ROUND((j.attitude_avg + 1.0) / 2.0, 4)
  END AS reddit_proxy_score,

  -- Status + confidence
  CASE
    WHEN COALESCE(j.confirmed_mentions, 0) = 0 THEN 'no_mentions'
    WHEN j.confirmed_mentions < 5 THEN 'insufficient_data'
    ELSE 'sufficient'
  END AS reddit_status,

  CASE
    WHEN COALESCE(j.confirmed_mentions, 0) < 5 THEN 'NONE'
    WHEN j.confirmed_mentions < 15 THEN 'LOW'
    WHEN j.confirmed_mentions < 50 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS reddit_signal_confidence,

  -- Volume + breakdown for the data trail
  COALESCE(j.confirmed_mentions, 0) AS confirmed_mentions,
  COALESCE(j.subreddit_spread, 0) AS subreddit_spread,
  COALESCE(j.positive_count, 0) AS positive_count,
  COALESCE(j.mentions_only_count, 0) AS mentions_only_count,
  COALESCE(j.divisive_count, 0) AS divisive_count,
  COALESCE(j.negative_count, 0) AS negative_count,
  j.avg_ip_affinity,
  j.top_subreddit,

  -- Human-readable reasoning string for the data trail
  CASE
    WHEN COALESCE(j.confirmed_mentions, 0) = 0 THEN
      CONCAT('No confirmed Reddit mentions in the 7 monitored D&D ',
             'subreddits over the last 30 days.')
    WHEN j.confirmed_mentions < 5 THEN
      CONCAT('Only ', CAST(j.confirmed_mentions AS STRING),
             ' confirmed mentions — too thin to score reliably.')
    ELSE
      CONCAT(
        CAST(j.confirmed_mentions AS STRING), ' confirmed mentions across ',
        CAST(j.subreddit_spread AS STRING), ' subreddits (top: r/',
        j.top_subreddit, '). Breakdown: ',
        CAST(j.positive_count AS STRING), ' positive / ',
        CAST(j.divisive_count AS STRING), ' divisive / ',
        CAST(j.negative_count AS STRING), ' negative / ',
        CAST(j.mentions_only_count AS STRING), ' mentions-only. ',
        'Attitude average: ',
        CAST(ROUND(j.attitude_avg, 2) AS STRING), '.'
      )
  END AS reddit_proxy_reasoning,

  -- Standardized output contract
  'community_reception' AS signal_type,
  'reddit_d_and_d_community' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM joined j
ORDER BY reddit_proxy_score DESC NULLS LAST;
