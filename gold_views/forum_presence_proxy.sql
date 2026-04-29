-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.forum_presence_proxy — Stage 7 of community_reception (v2)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "TTRPG-forum reception" scored from AI-Bouncer-confirmed top
-- URLs across the 4 major forums (EN World, GitP, RPG.net, Dragonsfoot).
-- The audience here is the "DM/whale demographic" — older, more system-
-- literate, more protective of D&D's identity. Per Gemini's framing:
-- "Reddit is full of Players. AO3 is full of Fans. Traditional forums
-- are full of Dungeon Masters."
--
-- ─── TWO-LAYER DISAMBIGUATION ──────────────────────────────────────────
--
-- This view consumes the intersection of two BQ tables:
--
--   forum_presence_counts        ← Layer 1 (co-term-gated CSE harvest)
--   forum_top_urls_classified    ← Layer 2 (Gemini Flash AI Bouncer
--                                   on title+snippet)
--
-- Layer 1 (in harvest_forum_presence.py): co-term gating + banned-
-- context post-filter on top URLs. Mirrors the Stage 5 Reddit pattern.
--
-- Layer 2 (in classify_forum_top_urls.py): per-URL Gemini Flash binary
-- is_about_ip + attitude classification. Stage 7a-i ("cheap path") uses
-- title+snippet only. Stage 7a-ii (deferred) will add Playwright thread-
-- body scrape for high-resolution sentiment + backlash narrative
-- extraction.
--
-- ─── SCORE FORMULA (SENTIMENT-WEIGHTED) ────────────────────────────────
--
-- Mirrors the Stage 5 reddit_reception_proxy + Stage 6 homebrew pattern:
-- confidence-weighted attitude average mapped to [0, 1].
--
--   attitude_score per URL:
--     positive       = +1.0
--     mentions_only  =  0.0
--     divisive       = -0.4
--     negative       = -1.0
--
--   attitude_avg = SUM(attitude * confidence) / SUM(confidence)
--
--   forum_presence_score = (attitude_avg + 1) / 2
--
-- Sentiment-weighted is the right semantic for a "reception" measure:
-- an IP with 10 negative forum threads gets a low score (DMs reject it),
-- an IP with 10 positive threads gets a high score (DMs embrace it),
-- mixed sentiment gets ~0.5.
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
-- Lower threshold than Stage 5 reddit_reception (>=1 vs >=5) because
-- each forum thread represents disproportionate effort — long-form
-- discussion forums, not quick-hit social feeds.
--
--   = 0 confirmed   →  NULL with status='no_confirmed_forum_signal'
--   1-2 confirmed   →  scored, confidence='LOW'
--   3-5 confirmed   →  scored, confidence='MEDIUM'
--   6+ confirmed    →  scored, confidence='HIGH'
--
-- ─── DATA TRAIL ───────────────────────────────────────────────────────
--
-- Surfaces both the disambiguation funnel (cse_total → after_banned_context
-- → about_ip) and the attitude breakdown (positive / negative / divisive /
-- mentions_only counts) plus per-forum confirmed bucketing.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.forum_presence_proxy` AS

WITH

  latest_per_ip AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.forum_presence_counts`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name ORDER BY harvested_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-(ip, url) classifications. Take the latest classification.
  -- ────────────────────────────────────────────────────────────────────
  classifications AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.forum_top_urls_classified`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, url ORDER BY classified_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Flatten top URLs per IP and join classifications. Map attitude
  -- string to numeric attitude_score.
  -- ────────────────────────────────────────────────────────────────────
  flattened_with_class AS (
    SELECT
      l.ip_name,
      t.url,
      t.title,
      t.forum_domain,
      c.is_about_ip,
      c.forum_attitude,
      c.confidence AS classifier_confidence,
      c.reasoning AS classifier_reasoning,
      CASE c.forum_attitude
        WHEN 'positive'      THEN  1.0
        WHEN 'mentions_only' THEN  0.0
        WHEN 'divisive'      THEN -0.4
        WHEN 'negative'      THEN -1.0
        ELSE NULL
      END AS attitude_score
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    LEFT JOIN classifications c
      ON c.ip_name = l.ip_name AND c.url = t.url
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Aggregate per-IP: counts at each stage of the funnel + attitude
  -- breakdown + per-forum confirmed bucketing.
  -- ────────────────────────────────────────────────────────────────────
  per_ip_agg AS (
    SELECT
      f.ip_name,
      COUNT(*)                             AS top_after_banned_context,
      COUNTIF(f.is_about_ip)               AS confirmed_about_ip_count,
      COUNTIF(f.is_about_ip AND f.forum_attitude = 'positive')      AS positive_count,
      COUNTIF(f.is_about_ip AND f.forum_attitude = 'mentions_only') AS mentions_only_count,
      COUNTIF(f.is_about_ip AND f.forum_attitude = 'divisive')      AS divisive_count,
      COUNTIF(f.is_about_ip AND f.forum_attitude = 'negative')      AS negative_count,
      SAFE_DIVIDE(
        SUM(IF(f.is_about_ip, f.attitude_score * f.classifier_confidence, NULL)),
        NULLIF(SUM(IF(f.is_about_ip, f.classifier_confidence, NULL)), 0)
      ) AS attitude_avg,
      COUNTIF(f.is_about_ip AND f.forum_domain = 'enworld.org')         AS enworld_confirmed,
      COUNTIF(f.is_about_ip AND f.forum_domain = 'forums.giantitp.com') AS giantitp_confirmed,
      COUNTIF(f.is_about_ip AND f.forum_domain = 'rpg.net')             AS rpgnet_confirmed,
      COUNTIF(f.is_about_ip AND f.forum_domain = 'dragonsfoot.org')     AS dragonsfoot_confirmed
    FROM flattened_with_class f
    GROUP BY f.ip_name
  ),

  -- Pick a confirmed-positive top URL for the data trail
  top_positive AS (
    SELECT
      f.ip_name,
      ARRAY_AGG(STRUCT(f.url, f.title, f.forum_domain)
                ORDER BY f.classifier_confidence DESC LIMIT 1)[OFFSET(0)] AS top_url
    FROM flattened_with_class f
    WHERE f.is_about_ip AND f.forum_attitude = 'positive'
    GROUP BY f.ip_name
  ),

  -- Fallback: any confirmed URL if no positives
  top_confirmed AS (
    SELECT
      f.ip_name,
      ARRAY_AGG(STRUCT(f.url, f.title, f.forum_domain)
                ORDER BY f.classifier_confidence DESC LIMIT 1)[OFFSET(0)] AS top_url
    FROM flattened_with_class f
    WHERE f.is_about_ip
    GROUP BY f.ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      l.total_results_combined  AS cse_total_results_estimate,
      l.harvested_at,
      l.top_thread_urls         AS top_thread_urls_after_banned_context,
      COALESCE(p.top_after_banned_context,    0) AS top_after_banned_context,
      COALESCE(p.confirmed_about_ip_count,    0) AS confirmed_about_ip_count,
      COALESCE(p.positive_count,              0) AS positive_count,
      COALESCE(p.mentions_only_count,         0) AS mentions_only_count,
      COALESCE(p.divisive_count,              0) AS divisive_count,
      COALESCE(p.negative_count,              0) AS negative_count,
      p.attitude_avg,
      COALESCE(p.enworld_confirmed,           0) AS enworld_confirmed,
      COALESCE(p.giantitp_confirmed,          0) AS giantitp_confirmed,
      COALESCE(p.rpgnet_confirmed,            0) AS rpgnet_confirmed,
      COALESCE(p.dragonsfoot_confirmed,       0) AS dragonsfoot_confirmed,
      COALESCE(tp.top_url, tc.top_url)        AS top_thread_struct
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN latest_per_ip l USING (ip_name)
    LEFT JOIN per_ip_agg p USING (ip_name)
    LEFT JOIN top_positive tp USING (ip_name)
    LEFT JOIN top_confirmed tc USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  CASE
    WHEN j.confirmed_about_ip_count = 0 THEN NULL
    ELSE ROUND((j.attitude_avg + 1.0) / 2.0, 4)
  END AS forum_presence_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN j.confirmed_about_ip_count = 0 THEN 'no_confirmed_forum_signal'
    ELSE 'sufficient'
  END AS forum_status,

  CASE
    WHEN j.confirmed_about_ip_count = 0 THEN 'NONE'
    WHEN j.confirmed_about_ip_count <= 2 THEN 'LOW'
    WHEN j.confirmed_about_ip_count <= 5 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS forum_signal_confidence,

  -- ─── DISAMBIGUATION FUNNEL (data trail) ───────────────────────────────
  COALESCE(j.cse_total_results_estimate, 0) AS cse_total_results_estimate,
  j.top_after_banned_context,
  j.confirmed_about_ip_count,

  -- ─── ATTITUDE BREAKDOWN ───────────────────────────────────────────────
  j.positive_count,
  j.mentions_only_count,
  j.divisive_count,
  j.negative_count,
  ROUND(COALESCE(j.attitude_avg, 0), 4) AS attitude_avg,

  -- ─── PER-FORUM CONFIRMED BUCKETING ────────────────────────────────────
  j.enworld_confirmed,
  j.giantitp_confirmed,
  j.rpgnet_confirmed,
  j.dragonsfoot_confirmed,

  -- ─── SAMPLE URL ──────────────────────────────────────────────────────
  j.top_thread_struct.url           AS top_thread_url,
  j.top_thread_struct.title         AS top_thread_title,
  j.top_thread_struct.forum_domain  AS top_thread_forum,
  j.top_thread_urls_after_banned_context AS top_thread_urls,
  j.harvested_at,

  -- ─── BACKWARDS-COMPAT COLUMN ALIASES ──────────────────────────────────
  -- The composite view reads these names; keep them stable.
  COALESCE(j.cse_total_results_estimate, 0) AS total_results_combined,
  j.enworld_confirmed AS enworld_top_hits,
  j.giantitp_confirmed AS giantitp_top_hits,
  j.rpgnet_confirmed AS rpgnet_top_hits,
  j.dragonsfoot_confirmed AS dragonsfoot_top_hits,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN j.confirmed_about_ip_count = 0 AND COALESCE(j.cse_total_results_estimate, 0) = 0 THEN
      'No forum-thread mentions in EN World / GitP / RPG.net / Dragonsfoot.'
    WHEN j.confirmed_about_ip_count = 0 THEN
      CONCAT(
        'No CONFIRMED forum threads about this IP. ',
        'Funnel: ~', CAST(j.cse_total_results_estimate AS STRING),
        ' CSE → ', CAST(j.top_after_banned_context AS STRING),
        ' post-banned-context → 0 about-IP. Likely false-positive name collision.'
      )
    ELSE
      CONCAT(
        CAST(j.confirmed_about_ip_count AS STRING),
        ' confirmed forum thread(s). Attitude: ',
        CAST(j.positive_count AS STRING),     ' positive / ',
        CAST(j.mentions_only_count AS STRING),' mentions / ',
        CAST(j.divisive_count AS STRING),     ' divisive / ',
        CAST(j.negative_count AS STRING),     ' negative. ',
        'Top URLs from: EN World ', CAST(j.enworld_confirmed AS STRING),
        ', GitP ', CAST(j.giantitp_confirmed AS STRING),
        ', RPG.net ', CAST(j.rpgnet_confirmed AS STRING),
        ', Dragonsfoot ', CAST(j.dragonsfoot_confirmed AS STRING),
        '. Sample: "',
        SUBSTR(COALESCE(j.top_thread_struct.title, ''), 1, 80),
        IF(LENGTH(COALESCE(j.top_thread_struct.title, '')) > 80, '..."', '"')
      )
  END AS forum_reasoning,

  -- Standardized output contract
  'community_reception'           AS signal_type,
  'forum_presence_disambiguated'  AS stream_name,
  CURRENT_DATE()                  AS snapshot_date

FROM joined j
ORDER BY forum_presence_score DESC NULLS LAST;
