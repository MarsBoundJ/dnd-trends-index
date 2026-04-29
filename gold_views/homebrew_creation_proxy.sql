-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.homebrew_creation_proxy — Stage 6 of community_reception
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "homebrew creation intensity" score, computed from existing
-- Stage 5 classified mentions filtered to r/UnearthedArcana — the
-- premier D&D-homebrew design subreddit.
--
-- ─── WHY THIS IS A DIFFERENT DIMENSION ────────────────────────────────
--
-- Reception (Stage 5 P1)        — "what D&D players SAY about IP X"
-- Acquisition (Stage 5 P2)      — "what IP X's fans say about D&D"
-- BGG (Stage 3)                 — "what tabletop gamers BUY of IP X"
-- AO3 (Stage 4)                 — "what fans WRITE in stories about IP X"
-- Homebrew (THIS STAGE)         — "what fans BUILD as game design"
--
-- Per ChatGPT's framing: "We don't just measure what fans say or
-- consume — we measure what they BUILD themselves when the official
-- product doesn't exist."
--
-- This is revealed mechanical demand under friction. Building a
-- homebrew subclass takes 5-15 hours of game design labor; people
-- only do that when they're "so desperate to play this IP that WotC
-- hasn't published it yet."
--
-- ─── DATA SOURCE ──────────────────────────────────────────────────────
--
-- We DO NOT need a new harvest. Our Stage 5 PRAW search already
-- captured posts from r/UnearthedArcana (one of the 7 D&D
-- subreddits we scan). The AI Bouncer already classified them with
-- crossover_attitude. The Stage 5 reception_proxy view averaged
-- across all 7 subreddits — this Stage 6 view filters to just
-- UnearthedArcana to extract the homebrew-creation slice.
--
-- v1 limitations:
--   - Only ~14 confirmed homebrew mentions across ~12 IPs.
--   - Doesn't capture D&D Beyond Homebrew "Adds to Collection" (would
--     require bookmarklet build).
--   - Doesn't capture GM Binder / Homebrewery PDFs (would require
--     Google Custom Search API).
--   These are v2 enhancements per the plan; v1 ships using existing data.
--
-- ─── SCORE FORMULA ────────────────────────────────────────────────────
--
-- Same architecture as Stage 5 reception_proxy: confidence-weighted
-- attitude average mapped to [0, 1].
--
--   attitude_score:
--     positive       = +1.0  (homebrew creation IS positive engagement)
--     mentions_only  =  0.0
--     divisive       = -0.4
--     negative       = -1.0
--
--   homebrew_creation_score = (attitude_avg + 1) / 2
--
-- ─── ABSTENTION ───────────────────────────────────────────────────────
--
-- Lower threshold than Stage 5 reception (>=1 vs >=5) because each
-- homebrew mention represents disproportionate effort. A single
-- well-rated subclass post IS a meaningful signal.
--
--   = 0 mentions  →  NULL with status='no_homebrew_signal'
--   1-2 mentions  →  scored with confidence='LOW'
--   3-5 mentions  →  scored with confidence='MEDIUM'
--   6+ mentions   →  scored with confidence='HIGH'
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.homebrew_creation_proxy` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Filter to r/UnearthedArcana confirmed-relevant mentions.
  -- (r/3d6 deliberately excluded — it's a character-build optimization
  -- sub, not a homebrew-design sub. Scope is purity over breadth.)
  -- ────────────────────────────────────────────────────────────────────
  homebrew_mentions AS (
    SELECT
      ip_name,
      post_id,
      subreddit,
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
      AND subreddit = 'UnearthedArcana'
  ),

  per_ip AS (
    SELECT
      ip_name,
      COUNT(*) AS homebrew_mention_count,
      COUNTIF(crossover_attitude = 'positive') AS homebrew_positive_count,
      COUNTIF(crossover_attitude = 'mentions_only') AS homebrew_mentions_only_count,
      COUNTIF(crossover_attitude = 'divisive') AS homebrew_divisive_count,
      COUNTIF(crossover_attitude = 'negative') AS homebrew_negative_count,
      SAFE_DIVIDE(
        SUM(attitude_score * confidence),
        NULLIF(SUM(confidence), 0)
      ) AS attitude_avg,
      MAX(post_id) AS sample_post_id
    FROM homebrew_mentions
    GROUP BY ip_name
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Pull the title of one sample post per IP (for the data trail —
  -- "Show me an example homebrew post for Tokyo Ghoul")
  -- ────────────────────────────────────────────────────────────────────
  sample_titles AS (
    SELECT
      h.ip_name,
      MAX(p.title) AS sample_post_title
    FROM homebrew_mentions h
    JOIN `dnd-trends-index.dnd_trends_raw.reddit_ub_candidate_posts` p
      ON p.post_id = h.post_id AND p.ip_name = h.ip_name
    WHERE h.crossover_attitude = 'positive'
    GROUP BY h.ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      p.homebrew_mention_count,
      p.homebrew_positive_count,
      p.homebrew_mentions_only_count,
      p.homebrew_divisive_count,
      p.homebrew_negative_count,
      p.attitude_avg,
      st.sample_post_title
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip p ON p.ip_name = s.ip_name
    LEFT JOIN sample_titles st ON st.ip_name = s.ip_name
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- The score
  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN NULL
    ELSE ROUND((j.attitude_avg + 1.0) / 2.0, 4)
  END AS homebrew_creation_score,

  -- Status + confidence
  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN 'no_homebrew_signal'
    ELSE 'sufficient'
  END AS homebrew_status,

  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN 'NONE'
    WHEN j.homebrew_mention_count <= 2 THEN 'LOW'
    WHEN j.homebrew_mention_count <= 5 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS homebrew_signal_confidence,

  -- Volume + breakdown
  COALESCE(j.homebrew_mention_count, 0) AS homebrew_mention_count,
  COALESCE(j.homebrew_positive_count, 0) AS homebrew_positive_count,
  COALESCE(j.homebrew_mentions_only_count, 0) AS homebrew_mentions_only_count,
  COALESCE(j.homebrew_divisive_count, 0) AS homebrew_divisive_count,
  COALESCE(j.homebrew_negative_count, 0) AS homebrew_negative_count,
  j.sample_post_title,

  -- Reasoning
  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN
      'No homebrew creation detected in r/UnearthedArcana over the last 30 days.'
    ELSE
      CONCAT(
        CAST(j.homebrew_mention_count AS STRING),
        ' homebrew creation signal(s) in r/UnearthedArcana. Sample: "',
        SUBSTR(COALESCE(j.sample_post_title, ''), 1, 80),
        IF(LENGTH(COALESCE(j.sample_post_title, '')) > 80, '..."', '"'),
        ' Breakdown: ',
        CAST(j.homebrew_positive_count AS STRING), ' positive / ',
        CAST(j.homebrew_mentions_only_count AS STRING), ' mentions / ',
        CAST(j.homebrew_divisive_count AS STRING), ' divisive / ',
        CAST(j.homebrew_negative_count AS STRING), ' negative.'
      )
  END AS homebrew_reasoning,

  -- Standardized output contract
  'community_reception' AS signal_type,
  'reddit_unearthedarcana_homebrew' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM joined j
ORDER BY homebrew_creation_score DESC NULLS LAST;
