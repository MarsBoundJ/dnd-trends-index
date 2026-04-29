-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.external_homebrew_proxy — Stage 6b of community_reception (v2)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "external homebrew document presence" computed from Google
-- Custom Search of the 2 major markdown-to-PDF homebrew tools:
--
--     gmbinder.com                  — long-form supplement publishing
--     homebrewery.naturalcrit.com   — markdown homebrew brewery
--
-- Anyone polishing a "5e <IP>" supplement to PDF almost always lands
-- on one of these two tools. So a count of "<IP>" results on these
-- sites is a proxy for "this IP has revealed external homebrew
-- demand" beyond what r/UnearthedArcana captures.
--
-- ─── WHY THIS COMPLEMENTS v1 (homebrew_creation_proxy) ────────────────
--
-- v1 Stage 6 (homebrew_creation_proxy) measures discussion intensity
-- on r/UnearthedArcana — covered ~11 IPs out of 142 because PRAW only
-- harvested ~30 days of posts.
--
-- This v2 sub-stream measures *artifact intensity* — the polished PDF
-- output that someone published rather than just discussed. Artifacts
-- accumulate over years; discussion is a 30-day window.
--
-- Together they form a 2-axis homebrew picture:
--   v1 UA Reddit       = "people are TALKING about brewing for X"
--   v2 GMB/Homebrewery = "people have PUBLISHED brews for X"
--
-- The combined view (homebrew_combined_proxy) blends them.
--
-- ─── SCORE FORMULA ─────────────────────────────────────────────────────
--
-- Log-scale normalize total_results_combined within the dataset:
--
--   external_homebrew_score = log10(total + 1) / log10(MAX + 1)
--
-- Same pattern as forum_presence_proxy. Heavy-tailed distribution
-- (mainstream IPs in hundreds, niche in single digits) is correctly
-- represented on a log scale.
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
-- Lower threshold than Reddit (>=1 vs >=5) because each PDF on
-- GMBinder/Homebrewery represents 5-15 hours of game-design labor.
-- A single "Unofficial 5e Berserk Supplement" on Homebrewery IS a
-- meaningful signal.
--
--   = 0 results          →  NULL with status='no_external_homebrew_signal'
--   1-5 results          →  scored with confidence='LOW'
--   6-25 results         →  scored with confidence='MEDIUM'
--   26+ results          →  scored with confidence='HIGH'
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

  max_results AS (
    SELECT MAX(total_results_combined) AS max_total
    FROM latest_per_ip
    WHERE total_results_combined > 0
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-platform bucketing of the top URLs.  Each top_thread_urls entry
  -- has a source_domain field; UNNEST + COUNTIF gives us per-platform
  -- hit counts (out of top 10 results).
  -- ────────────────────────────────────────────────────────────────────
  per_platform_buckets AS (
    SELECT
      l.ip_name,
      COUNTIF(t.source_domain = 'gmbinder.com')                AS gmbinder_top_hits,
      COUNTIF(t.source_domain = 'homebrewery.naturalcrit.com') AS homebrewery_top_hits
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    GROUP BY l.ip_name
  ),

  top_url_per_ip AS (
    SELECT
      l.ip_name,
      ARRAY_AGG(STRUCT(t.url, t.title, t.source_domain) ORDER BY t.source_domain LIMIT 1)[OFFSET(0)] AS top_url
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    GROUP BY l.ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      l.total_results_combined,
      l.harvested_at,
      l.top_thread_urls,
      pp.gmbinder_top_hits,
      pp.homebrewery_top_hits,
      tu.top_url AS top_url_struct
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN latest_per_ip l USING (ip_name)
    LEFT JOIN per_platform_buckets pp USING (ip_name)
    LEFT JOIN top_url_per_ip tu USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN NULL
    ELSE ROUND(
      SAFE_DIVIDE(
        LOG10(j.total_results_combined + 1),
        LOG10((SELECT max_total FROM max_results) + 1)
      ),
      4
    )
  END AS external_homebrew_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN 'no_external_homebrew_signal'
    ELSE 'sufficient'
  END AS external_homebrew_status,

  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN 'NONE'
    WHEN j.total_results_combined <= 5 THEN 'LOW'
    WHEN j.total_results_combined <= 25 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS external_homebrew_signal_confidence,

  -- ─── DATA TRAIL ───────────────────────────────────────────────────────
  COALESCE(j.total_results_combined, 0)  AS external_homebrew_total_results,
  COALESCE(j.gmbinder_top_hits, 0)       AS gmbinder_top_hits,
  COALESCE(j.homebrewery_top_hits, 0)    AS homebrewery_top_hits,
  j.top_url_struct.url                   AS top_homebrew_url,
  j.top_url_struct.title                 AS top_homebrew_title,
  j.top_url_struct.source_domain         AS top_homebrew_source,
  j.top_thread_urls                      AS external_homebrew_top_urls,
  j.harvested_at                         AS external_homebrew_harvested_at,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN
      'No homebrew artifacts found on GMBinder or Homebrewery for this IP.'
    ELSE
      CONCAT(
        CAST(j.total_results_combined AS STRING),
        ' external homebrew artifact(s) on GMBinder/Homebrewery. ',
        'Top URLs from: GMBinder ', CAST(COALESCE(j.gmbinder_top_hits, 0) AS STRING),
        ', Homebrewery ', CAST(COALESCE(j.homebrewery_top_hits, 0) AS STRING),
        '. Sample: "',
        SUBSTR(COALESCE(j.top_url_struct.title, ''), 1, 80),
        IF(LENGTH(COALESCE(j.top_url_struct.title, '')) > 80, '..."', '"')
      )
  END AS external_homebrew_reasoning,

  -- Standardized output contract
  'community_reception' AS signal_type,
  'external_homebrew_google_cse' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM joined j
ORDER BY external_homebrew_score DESC NULLS LAST;
