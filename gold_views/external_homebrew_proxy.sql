-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.external_homebrew_proxy — Stage 6b of community_reception (v2)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "external homebrew artifact intensity" scored from
-- AI-Bouncer-confirmed top URLs on the 2 major markdown-to-PDF
-- homebrew tools:
--
--     gmbinder.com                  — long-form supplement publishing
--     homebrewery.naturalcrit.com   — markdown homebrew brewery
--
-- ─── TWO-LAYER DISAMBIGUATION ──────────────────────────────────────────
--
-- This view consumes the intersection of two BQ tables:
--
--   external_homebrew_presence_counts   ← Layer 1 (co-term-gated CSE)
--   external_homebrew_classified        ← Layer 2 (Gemini Flash AI Bouncer)
--
-- The score is computed from CONFIRMED top URLs only — those where
-- the AI Bouncer says is_about_ip=TRUE AND is_5e_homebrew=TRUE.
--
-- Why both flags:
--   is_about_ip       — disambiguates "Tyranny" (game) from
--                        "tyranny" (generic English noun)
--   is_5e_homebrew    — separates real D&D 5e homebrew from book PDFs,
--                        cookbooks, art-of books, Star Wars 5e (SW5e),
--                        Pathfinder docs, etc. that happen to live on
--                        GMBinder/Homebrewery hosting infrastructure
--
-- Stage 6b v0 (Apr 29 morning) shipped without disambiguation and
-- inflated Tyranny / Invincible / The Boys via coincidental keyword
-- matches and non-homebrew PDFs hosted on these platforms. v1 (this
-- file) corrects that.
--
-- ─── SCORE FORMULA (CONSERVATIVE) ──────────────────────────────────────
--
--   confirmed_5e_homebrew_count = COUNT of top URLs where
--     is_about_ip=TRUE AND is_5e_homebrew=TRUE
--
--   external_homebrew_score = LOG10(confirmed + 1) / LOG10(11)
--
-- The denominator is fixed at LOG10(11) because the maximum possible
-- confirmed_count is 10 (we capture top 10 URLs per IP). This is the
-- "conservative" approach — we score from what we observed in the
-- top 10, never extrapolate from CSE's total_results_combined estimate.
--
-- Score curve:
--   0 confirmed  →  NULL  (abstention)
--   1 confirmed  →  0.289
--   2 confirmed  →  0.458
--   5 confirmed  →  0.747
--   10 confirmed →  1.000
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
--   0 confirmed   →  NULL with status='no_confirmed_homebrew_signal'
--   1 confirmed   →  scored, confidence='LOW'
--   2-4 confirmed →  scored, confidence='MEDIUM'
--   5+ confirmed  →  scored, confidence='HIGH'
--
-- ─── DATA TRAIL ───────────────────────────────────────────────────────
--
-- Surfaces both layers' counts so reviewers can see the disambiguation
-- funnel: cse_total → after_banned_context → about_ip → 5e_homebrew.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.external_homebrew_proxy` AS

WITH

  latest_per_ip AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.external_homebrew_presence_counts`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name ORDER BY harvested_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-(ip, url) classifications. Take the latest classification for
  -- any (ip, url) pair (handles re-classification with --force).
  -- ────────────────────────────────────────────────────────────────────
  classifications AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.external_homebrew_classified`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, url ORDER BY classified_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Flatten top URLs per IP and join classifications
  -- ────────────────────────────────────────────────────────────────────
  flattened_with_class AS (
    SELECT
      l.ip_name,
      t.url,
      t.title,
      t.source_domain,
      c.is_about_ip,
      c.is_5e_homebrew,
      c.confidence AS classifier_confidence,
      c.reasoning AS classifier_reasoning
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    LEFT JOIN classifications c
      ON c.ip_name = l.ip_name AND c.url = t.url
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Aggregate per-IP: count surviving top URLs at each disambiguation
  -- layer, plus per-platform breakdown.
  -- ────────────────────────────────────────────────────────────────────
  per_ip_agg AS (
    SELECT
      f.ip_name,
      COUNT(*) AS top_after_banned_context,
      COUNTIF(f.is_about_ip)                                     AS confirmed_about_ip_count,
      COUNTIF(f.is_about_ip AND f.is_5e_homebrew)                AS confirmed_5e_homebrew_count,
      COUNTIF(f.is_5e_homebrew AND f.source_domain = 'gmbinder.com')                AS gmbinder_confirmed,
      COUNTIF(f.is_5e_homebrew AND f.source_domain = 'homebrewery.naturalcrit.com') AS homebrewery_confirmed
    FROM flattened_with_class f
    GROUP BY f.ip_name
  ),

  -- Sample one confirmed URL for the data trail (best-confidence first)
  top_confirmed AS (
    SELECT
      f.ip_name,
      ARRAY_AGG(STRUCT(f.url, f.title, f.source_domain)
                ORDER BY f.classifier_confidence DESC LIMIT 1)[OFFSET(0)] AS top_confirmed_url
    FROM flattened_with_class f
    WHERE f.is_about_ip AND f.is_5e_homebrew
    GROUP BY f.ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      l.total_results_combined          AS cse_total_results_estimate,
      l.harvested_at,
      l.top_thread_urls                 AS top_thread_urls_after_banned_context,
      COALESCE(p.top_after_banned_context,    0) AS top_after_banned_context,
      COALESCE(p.confirmed_about_ip_count,    0) AS confirmed_about_ip_count,
      COALESCE(p.confirmed_5e_homebrew_count, 0) AS confirmed_5e_homebrew_count,
      COALESCE(p.gmbinder_confirmed,          0) AS gmbinder_confirmed,
      COALESCE(p.homebrewery_confirmed,       0) AS homebrewery_confirmed,
      tc.top_confirmed_url
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN latest_per_ip l USING (ip_name)
    LEFT JOIN per_ip_agg p USING (ip_name)
    LEFT JOIN top_confirmed tc USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  CASE
    WHEN j.confirmed_5e_homebrew_count = 0 THEN NULL
    ELSE ROUND(LOG10(j.confirmed_5e_homebrew_count + 1) / LOG10(11), 4)
  END AS external_homebrew_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN j.confirmed_5e_homebrew_count = 0 THEN 'no_confirmed_homebrew_signal'
    ELSE 'sufficient'
  END AS external_homebrew_status,

  CASE
    WHEN j.confirmed_5e_homebrew_count = 0 THEN 'NONE'
    WHEN j.confirmed_5e_homebrew_count = 1 THEN 'LOW'
    WHEN j.confirmed_5e_homebrew_count <= 4 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS external_homebrew_signal_confidence,

  -- ─── DISAMBIGUATION FUNNEL (data trail) ───────────────────────────────
  -- Stages of the funnel from raw CSE → confirmed homebrew artifact:
  COALESCE(j.cse_total_results_estimate, 0)  AS cse_total_results_estimate,  -- unfiltered, often inflated
  j.top_after_banned_context                  AS top_after_banned_context,    -- L1 banned-context drop
  j.confirmed_about_ip_count                  AS confirmed_about_ip_count,    -- L2 is_about_ip=TRUE
  j.confirmed_5e_homebrew_count               AS confirmed_5e_homebrew_count, -- L2 BOTH flags TRUE (the score input)

  -- Per-platform confirmed breakdown
  j.gmbinder_confirmed,
  j.homebrewery_confirmed,

  -- Sample of a confirmed homebrew artifact
  j.top_confirmed_url.url    AS top_confirmed_homebrew_url,
  j.top_confirmed_url.title  AS top_confirmed_homebrew_title,
  j.top_confirmed_url.source_domain AS top_confirmed_homebrew_source,
  j.top_thread_urls_after_banned_context AS external_homebrew_top_urls,
  j.harvested_at AS external_homebrew_harvested_at,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN j.confirmed_5e_homebrew_count = 0 AND COALESCE(j.cse_total_results_estimate, 0) = 0 THEN
      'No GMBinder/Homebrewery search results for this IP.'
    WHEN j.confirmed_5e_homebrew_count = 0 THEN
      CONCAT(
        'No CONFIRMED 5e homebrew artifacts. ',
        'CSE returned ~', CAST(j.cse_total_results_estimate AS STRING),
        ' raw results, ', CAST(j.top_after_banned_context AS STRING),
        ' survived the banned-context filter, ',
        CAST(j.confirmed_about_ip_count AS STRING),
        ' were genuinely about the IP, but 0 were 5e homebrew (likely book PDFs / non-D&D systems).'
      )
    ELSE
      CONCAT(
        CAST(j.confirmed_5e_homebrew_count AS STRING),
        ' confirmed 5e homebrew artifact(s) in top 10. ',
        'Funnel: ~', CAST(j.cse_total_results_estimate AS STRING),
        ' CSE → ', CAST(j.top_after_banned_context AS STRING),
        ' post-banned-context → ', CAST(j.confirmed_about_ip_count AS STRING),
        ' about-IP → ', CAST(j.confirmed_5e_homebrew_count AS STRING),
        ' 5e-homebrew. Sample: "',
        SUBSTR(COALESCE(j.top_confirmed_url.title, ''), 1, 80),
        IF(LENGTH(COALESCE(j.top_confirmed_url.title, '')) > 80, '..."', '"')
      )
  END AS external_homebrew_reasoning,

  -- Standardized output contract
  'community_reception' AS signal_type,
  'external_homebrew_disambiguated' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM joined j
ORDER BY external_homebrew_score DESC NULLS LAST;
