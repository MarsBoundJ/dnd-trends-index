-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.homebrew_combined_proxy — Stage 6 unified (v1 + v2 sub-stream)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Blends two complementary homebrew signals into a single per-IP score
-- that the UB Matrix composite consumes:
--
--   v1 sub-signal — homebrew_creation_proxy
--     Reddit r/UnearthedArcana classified mention intensity.
--     Sentiment-aware (positive/divisive/negative weighting).
--     Captures "people are TALKING about brewing for X" in 30-day window.
--     Coverage: ~11/142 IPs (thin — limited by PRAW window).
--
--   v2 sub-signal — external_homebrew_proxy (disambiguated, two-layer)
--     Google CSE count of CONFIRMED 5e homebrew artifacts on
--     GMBinder + Homebrewery, after Layer 1 (co-term gating + banned-
--     context filter) and Layer 2 (Gemini Flash AI Bouncer
--     is_about_ip + is_5e_homebrew classification). Score is from
--     the confirmed top-10 count, not raw CSE total.
--     Captures "people have PUBLISHED 5e brews for X" — artifacts
--     accumulate over years.
--
-- ─── BLENDING RULE ─────────────────────────────────────────────────────
--
-- Equal-weighted average with per-IP renormalization (same pattern as
-- the master composite):
--
--   both present       →  AVG(v1, v2)
--   only v1 present    →  v1 (Reddit UA discussion only)
--   only v2 present    →  v2 (external artifacts only)
--   neither present    →  NULL
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.homebrew_combined_proxy` AS

WITH

  ua_signal AS (
    SELECT
      ip_name,
      homebrew_creation_score AS ua_homebrew_score,
      homebrew_status        AS ua_homebrew_status,
      homebrew_mention_count AS ua_homebrew_mention_count,
      homebrew_positive_count,
      homebrew_mentions_only_count,
      homebrew_divisive_count,
      homebrew_negative_count,
      sample_post_title      AS ua_sample_post_title
    FROM `dnd-trends-index.gold_data.homebrew_creation_proxy`
  ),

  external_signal AS (
    SELECT
      ip_name,
      external_homebrew_score,
      external_homebrew_status,
      cse_total_results_estimate,
      top_after_banned_context,
      confirmed_about_ip_count,
      confirmed_5e_homebrew_count,
      gmbinder_confirmed,
      homebrewery_confirmed,
      top_confirmed_homebrew_url,
      top_confirmed_homebrew_title,
      top_confirmed_homebrew_source,
      external_homebrew_top_urls
    FROM `dnd-trends-index.gold_data.external_homebrew_proxy`
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      ua.ua_homebrew_score,
      ua.ua_homebrew_status,
      ua.ua_homebrew_mention_count,
      ua.homebrew_positive_count,
      ua.homebrew_mentions_only_count,
      ua.homebrew_divisive_count,
      ua.homebrew_negative_count,
      ua.ua_sample_post_title,
      ext.external_homebrew_score,
      ext.external_homebrew_status,
      ext.cse_total_results_estimate,
      ext.top_after_banned_context,
      ext.confirmed_about_ip_count,
      ext.confirmed_5e_homebrew_count,
      ext.gmbinder_confirmed,
      ext.homebrewery_confirmed,
      ext.top_confirmed_homebrew_url,
      ext.top_confirmed_homebrew_title,
      ext.top_confirmed_homebrew_source,
      ext.external_homebrew_top_urls
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN ua_signal ua USING (ip_name)
    LEFT JOIN external_signal ext USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE COMBINED SCORE ───────────────────────────────────────────────
  CASE
    WHEN j.ua_homebrew_score IS NOT NULL
         AND j.external_homebrew_score IS NOT NULL THEN
      ROUND((j.ua_homebrew_score + j.external_homebrew_score) / 2.0, 4)
    WHEN j.ua_homebrew_score IS NOT NULL THEN
      j.ua_homebrew_score
    WHEN j.external_homebrew_score IS NOT NULL THEN
      j.external_homebrew_score
    ELSE NULL
  END AS homebrew_combined_score,

  -- ─── COMBINED STATUS ──────────────────────────────────────────────────
  CASE
    WHEN j.ua_homebrew_score IS NOT NULL
         AND j.external_homebrew_score IS NOT NULL THEN 'sufficient_both'
    WHEN j.ua_homebrew_score IS NOT NULL THEN 'sufficient_ua_only'
    WHEN j.external_homebrew_score IS NOT NULL THEN 'sufficient_external_only'
    ELSE 'no_homebrew_signal'
  END AS homebrew_combined_status,

  (
    IF(j.ua_homebrew_score IS NOT NULL, 1, 0) +
    IF(j.external_homebrew_score IS NOT NULL, 1, 0)
  ) AS homebrew_sub_signals_present,

  -- ─── PASS-THROUGH SUB-SIGNALS (data trail) ────────────────────────────
  -- v1 UA Reddit
  j.ua_homebrew_score,
  COALESCE(j.ua_homebrew_mention_count, 0) AS ua_homebrew_mention_count,
  COALESCE(j.homebrew_positive_count, 0)      AS homebrew_positive_count,
  COALESCE(j.homebrew_mentions_only_count, 0) AS homebrew_mentions_only_count,
  COALESCE(j.homebrew_divisive_count, 0)      AS homebrew_divisive_count,
  COALESCE(j.homebrew_negative_count, 0)      AS homebrew_negative_count,
  j.ua_sample_post_title,

  -- v2 external (disambiguated)
  j.external_homebrew_score,
  COALESCE(j.cse_total_results_estimate, 0)  AS cse_total_results_estimate,
  COALESCE(j.top_after_banned_context, 0)    AS top_after_banned_context,
  COALESCE(j.confirmed_about_ip_count, 0)    AS confirmed_about_ip_count,
  COALESCE(j.confirmed_5e_homebrew_count, 0) AS confirmed_5e_homebrew_count,
  COALESCE(j.gmbinder_confirmed, 0)          AS gmbinder_confirmed,
  COALESCE(j.homebrewery_confirmed, 0)       AS homebrewery_confirmed,
  j.top_confirmed_homebrew_url,
  j.top_confirmed_homebrew_title,
  j.top_confirmed_homebrew_source,
  j.external_homebrew_top_urls,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN j.ua_homebrew_score IS NULL AND j.external_homebrew_score IS NULL THEN
      'No homebrew signal — neither r/UnearthedArcana mentions nor confirmed GMBinder/Homebrewery 5e artifacts.'
    WHEN j.ua_homebrew_score IS NOT NULL AND j.external_homebrew_score IS NOT NULL THEN
      CONCAT(
        'Combined homebrew signal: UA Reddit ',
        CAST(ROUND(j.ua_homebrew_score, 2) AS STRING),
        ' (', CAST(j.ua_homebrew_mention_count AS STRING), ' mentions) + ',
        'External ', CAST(ROUND(j.external_homebrew_score, 2) AS STRING),
        ' (', CAST(j.confirmed_5e_homebrew_count AS STRING),
        ' confirmed 5e artifacts).'
      )
    WHEN j.ua_homebrew_score IS NOT NULL THEN
      CONCAT(
        'UA Reddit homebrew only: ',
        CAST(j.ua_homebrew_mention_count AS STRING),
        ' classified mentions. No confirmed 5e artifacts on GMBinder/Homebrewery.'
      )
    ELSE
      CONCAT(
        'External homebrew only: ',
        CAST(j.confirmed_5e_homebrew_count AS STRING),
        ' confirmed 5e artifact(s) on GMBinder/Homebrewery. No r/UnearthedArcana classified mentions.'
      )
  END AS homebrew_combined_reasoning,

  -- Standardized output contract
  'community_reception'    AS signal_type,
  'homebrew_combined'      AS stream_name,
  CURRENT_DATE()           AS snapshot_date

FROM joined j
ORDER BY homebrew_combined_score DESC NULLS LAST;
