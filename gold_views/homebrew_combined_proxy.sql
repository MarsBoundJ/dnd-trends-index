-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.homebrew_combined_proxy — Stage 6 unified (3 sub-streams)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Blends THREE complementary homebrew signals into a single per-IP
-- score that the UB Matrix composite consumes:
--
--   v1 sub-signal — homebrew_creation_proxy
--     Reddit r/UnearthedArcana classified mention intensity.
--     Sentiment-aware (positive/divisive/negative weighting).
--     "people are TALKING about brewing for X" — 30-day discussion window.
--     Coverage: ~11/142 IPs (thin — limited by PRAW window).
--
--   v2a sub-signal — external_homebrew_proxy (Stage 6b, disambiguated)
--     Google CSE count of CONFIRMED 5e homebrew artifacts on
--     GMBinder + Homebrewery, after Layer 1 (co-term gating + banned-
--     context filter) and Layer 2 (Gemini Flash AI Bouncer
--     is_about_ip + is_5e_homebrew classification). Score is from
--     the confirmed top-10 count.
--     "people have PUBLISHED 5e brews for X" — artifacts accumulate
--     over years on external publishing tools.
--
--   v2b sub-signal — ddb_homebrew_proxy (Stage 6a, NEW Apr 29 evening)
--     D&D Beyond native homebrew — bookmarklet-captured visible items
--     across 5 priority sections (subclasses, spells, monsters,
--     magic-items, species). Score is from total_items aggregated
--     across sections, log-normalized.
--     "people have BUILT and SHARED 5e content for X on the native
--     D&D platform" — strongest "I want to play this at the table
--     TODAY" signal in the matrix.
--     v1 ships raw counts; v2 (Stage 6c) will add an AI Bouncer pass
--     to disambiguate fuzzy filter-name matches.
--
-- ─── BLENDING RULE ─────────────────────────────────────────────────────
--
-- Equal-weighted average across the present sub-signals. NULL signals
-- drop out and the average is taken only over the present ones (per-
-- IP renormalization, same pattern as the master composite).
--
--   3 present →  AVG(ua, ext, ddb)
--   2 present →  AVG(of the present pair)
--   1 present →  use that one
--   0 present →  NULL
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

  ddb_signal AS (
    SELECT
      ip_name,
      ddb_homebrew_score,
      ddb_homebrew_status,
      ddb_total_items,
      ddb_subclasses_items,
      ddb_spells_items,
      ddb_monsters_items,
      ddb_magic_items_items,
      ddb_species_items,
      ddb_sections_captured,
      ddb_top_item_name,
      ddb_top_item_url,
      ddb_top_item_section,
      ddb_top_item_adds,
      ddb_last_captured_at
    FROM `dnd-trends-index.gold_data.ddb_homebrew_proxy`
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
      ext.external_homebrew_top_urls,
      ddb.ddb_homebrew_score,
      ddb.ddb_homebrew_status,
      ddb.ddb_total_items,
      ddb.ddb_subclasses_items,
      ddb.ddb_spells_items,
      ddb.ddb_monsters_items,
      ddb.ddb_magic_items_items,
      ddb.ddb_species_items,
      ddb.ddb_sections_captured,
      ddb.ddb_top_item_name,
      ddb.ddb_top_item_url,
      ddb.ddb_top_item_section,
      ddb.ddb_top_item_adds,
      ddb.ddb_last_captured_at
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN ua_signal       ua  USING (ip_name)
    LEFT JOIN external_signal ext USING (ip_name)
    LEFT JOIN ddb_signal      ddb USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE COMBINED SCORE (3-way average with renormalization) ──────────
  ROUND(
    SAFE_DIVIDE(
      COALESCE(j.ua_homebrew_score,       0)
      + COALESCE(j.external_homebrew_score, 0)
      + COALESCE(j.ddb_homebrew_score,      0),
      NULLIF(
        IF(j.ua_homebrew_score       IS NOT NULL, 1, 0) +
        IF(j.external_homebrew_score IS NOT NULL, 1, 0) +
        IF(j.ddb_homebrew_score      IS NOT NULL, 1, 0),
        0
      )
    ),
    4
  ) AS homebrew_combined_score,

  -- ─── COMBINED STATUS ──────────────────────────────────────────────────
  CASE
    WHEN j.ua_homebrew_score IS NULL
         AND j.external_homebrew_score IS NULL
         AND j.ddb_homebrew_score IS NULL
      THEN 'no_homebrew_signal'
    WHEN (
      IF(j.ua_homebrew_score       IS NOT NULL, 1, 0) +
      IF(j.external_homebrew_score IS NOT NULL, 1, 0) +
      IF(j.ddb_homebrew_score      IS NOT NULL, 1, 0)
    ) = 3 THEN 'sufficient_all_three'
    WHEN (
      IF(j.ua_homebrew_score       IS NOT NULL, 1, 0) +
      IF(j.external_homebrew_score IS NOT NULL, 1, 0) +
      IF(j.ddb_homebrew_score      IS NOT NULL, 1, 0)
    ) = 2 THEN 'sufficient_two'
    ELSE 'sufficient_one'
  END AS homebrew_combined_status,

  (
    IF(j.ua_homebrew_score       IS NOT NULL, 1, 0) +
    IF(j.external_homebrew_score IS NOT NULL, 1, 0) +
    IF(j.ddb_homebrew_score      IS NOT NULL, 1, 0)
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

  -- v2a external
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

  -- v2b DDB native (NEW)
  j.ddb_homebrew_score,
  COALESCE(j.ddb_total_items,         0) AS ddb_total_items,
  COALESCE(j.ddb_subclasses_items,    0) AS ddb_subclasses_items,
  COALESCE(j.ddb_spells_items,        0) AS ddb_spells_items,
  COALESCE(j.ddb_monsters_items,      0) AS ddb_monsters_items,
  COALESCE(j.ddb_magic_items_items,   0) AS ddb_magic_items_items,
  COALESCE(j.ddb_species_items,       0) AS ddb_species_items,
  COALESCE(j.ddb_sections_captured,   0) AS ddb_sections_captured,
  j.ddb_top_item_name,
  j.ddb_top_item_url,
  j.ddb_top_item_section,
  j.ddb_top_item_adds,
  j.ddb_last_captured_at,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CONCAT(
    CASE
      WHEN j.ua_homebrew_score IS NULL
           AND j.external_homebrew_score IS NULL
           AND j.ddb_homebrew_score IS NULL THEN
        'No homebrew signal across UA Reddit, GMBinder/Homebrewery, or D&D Beyond.'
      ELSE 'Combined homebrew signal: '
    END,
    IF(j.ua_homebrew_score IS NOT NULL,
       CONCAT('UA Reddit ', CAST(ROUND(j.ua_homebrew_score, 2) AS STRING),
              ' (', CAST(j.ua_homebrew_mention_count AS STRING), ' mentions). '),
       ''),
    IF(j.external_homebrew_score IS NOT NULL,
       CONCAT('External ', CAST(ROUND(j.external_homebrew_score, 2) AS STRING),
              ' (', CAST(j.confirmed_5e_homebrew_count AS STRING),
              ' confirmed 5e artifacts). '),
       ''),
    IF(j.ddb_homebrew_score IS NOT NULL,
       CONCAT('DDB ', CAST(ROUND(j.ddb_homebrew_score, 2) AS STRING),
              ' (', CAST(j.ddb_total_items AS STRING), ' items). '),
       '')
  ) AS homebrew_combined_reasoning,

  -- Standardized output contract
  'community_reception'    AS signal_type,
  'homebrew_combined_v3'   AS stream_name,
  CURRENT_DATE()           AS snapshot_date

FROM joined j
ORDER BY homebrew_combined_score DESC NULLS LAST;
