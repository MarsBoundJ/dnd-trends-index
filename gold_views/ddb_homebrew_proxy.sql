-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.ddb_homebrew_proxy — Stage 6a of community_reception (v2)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "DDB-native homebrew artifact intensity" scored from AI-Bouncer-
-- confirmed items in dnd_trends_raw.ddb_homebrew_counts joined to
-- dnd_trends_raw.ddb_homebrew_classified.
--
-- D&D Beyond is the native D&D ecosystem — homebrew here is the
-- strongest "I want to play this IP at the table TODAY" signal in the
-- matrix. Per the Stage 6 plan, all 3 reviewer tools (ChatGPT / Gemini
-- / Perplexity) rated DDB Homebrew the #1 priority enrichment.
--
-- ─── TWO-LAYER DISAMBIGUATION (v2 = Stage 6c) ──────────────────────────
--
-- Layer 1 (in the bookmarklet): per-section filter param mapped via
-- SECTION_FILTER_PARAM. DDB has two filter-form generations —
-- filter-name for newer 5e-2024 character-creation sections
-- (subclasses/species/feats/backgrounds), filter-search for older
-- content sections (spells/monsters/magic-items).
--
-- Layer 2 (Stage 6c, this view consumes it): Gemini Flash AI Bouncer
-- per-item is_about_ip classification. DDB's filter does fuzzy
-- matching across name + tags + description, so some captured items
-- are not actually about the IP — Hades captures included a generic
-- "Demigod" species with 4440 adds that's universally used for
-- Greek/Asgard themes; Foundation included "School of Foundation
-- Magic" (generic foundation-of-magic theme); Pantheon included
-- "Pandora's Box (Pantheon Campaign)" (generic mythology). The AI
-- Bouncer classifies each item and this view filters to is_about_ip = TRUE.
--
-- ─── SCORE FORMULA (CONFIRMED-COUNT) ──────────────────────────────────
--
-- Aggregate confirmed_about_ip_count across all 5 priority sections per
-- IP. Log-normalize against the dataset-wide max:
--
--   ddb_homebrew_score = LOG10(confirmed_total + 1) / LOG10(MAX_total + 1)
--
-- Same heavy-tailed pattern as Stage 6b external_homebrew_proxy v1.
-- Conservative: scores from CONFIRMED items only, never inflated by
-- fuzzy-match coincidences.
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
--   confirmed_total = 0  →  NULL with status='no_confirmed_ddb_signal'
--   1-3 confirmed        →  scored, confidence='LOW'
--   4-15 confirmed       →  scored, confidence='MEDIUM'
--   16+ confirmed        →  scored, confidence='HIGH'
--
-- ─── DATA TRAIL ───────────────────────────────────────────────────────
--
-- Surfaces both the raw and confirmed counts so reviewers can see the
-- disambiguation funnel: visible_items → about_ip → score.
-- Plus per-section breakdowns + top confirmed item per IP.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.ddb_homebrew_proxy` AS

WITH

  latest_per_ip_section AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.ddb_homebrew_counts`
    WHERE NOT (
      scraped_by = 'ddb_homebrew_bookmarklet_bulk'
      AND ddb_section IN ('spells', 'monsters', 'magic-items')
      AND scraped_at < TIMESTAMP('2026-04-29T23:00:00Z')
    )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, ddb_section ORDER BY scraped_at DESC
    ) = 1
  ),

  classifications AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.ddb_homebrew_classified`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, slug ORDER BY classified_at DESC
    ) = 1
  ),

  -- Flatten top_items, join classifications. Items without a
  -- classification yet are conservatively treated as is_about_ip = FALSE
  -- (they don't contribute to the score until classified).
  flattened AS (
    SELECT
      l.ip_name,
      l.ddb_section,
      t.name,
      t.slug,
      t.url,
      t.adds,
      t.base_class,
      COALESCE(c.is_about_ip, FALSE) AS is_about_ip,
      c.confidence AS classifier_confidence,
      c.reasoning AS classifier_reasoning
    FROM latest_per_ip_section l, UNNEST(l.top_items) AS t
    LEFT JOIN classifications c
      ON c.ip_name = l.ip_name AND c.slug = t.slug
    WHERE l.ddb_section IN ('subclasses','spells','monsters','magic-items','species')
      AND t.slug IS NOT NULL AND t.slug != ''
  ),

  -- Aggregate per IP: confirmed counts by section + grand totals
  per_ip AS (
    SELECT
      ip_name,
      COUNT(*) AS visible_total,
      COUNTIF(is_about_ip) AS confirmed_total,
      COUNTIF(ddb_section = 'subclasses')                       AS visible_subclasses,
      COUNTIF(ddb_section = 'spells')                           AS visible_spells,
      COUNTIF(ddb_section = 'monsters')                         AS visible_monsters,
      COUNTIF(ddb_section = 'magic-items')                      AS visible_magic_items,
      COUNTIF(ddb_section = 'species')                          AS visible_species,
      COUNTIF(is_about_ip AND ddb_section = 'subclasses')       AS confirmed_subclasses,
      COUNTIF(is_about_ip AND ddb_section = 'spells')           AS confirmed_spells,
      COUNTIF(is_about_ip AND ddb_section = 'monsters')         AS confirmed_monsters,
      COUNTIF(is_about_ip AND ddb_section = 'magic-items')      AS confirmed_magic_items,
      COUNTIF(is_about_ip AND ddb_section = 'species')          AS confirmed_species,
      COUNT(DISTINCT ddb_section) AS sections_captured
    FROM flattened
    GROUP BY ip_name
  ),

  -- Top single CONFIRMED item per IP (highest adds among is_about_ip=TRUE)
  top_confirmed_item AS (
    SELECT
      ip_name,
      ARRAY_AGG(STRUCT(name, slug, url, ddb_section, adds, classifier_confidence)
                ORDER BY adds DESC LIMIT 1)[OFFSET(0)] AS top_item
    FROM flattened
    WHERE is_about_ip
    GROUP BY ip_name
  ),

  -- Capture timestamp for the data trail
  last_capture AS (
    SELECT ip_name, MAX(scraped_at) AS last_captured_at
    FROM latest_per_ip_section
    GROUP BY ip_name
  ),

  max_total AS (
    SELECT MAX(confirmed_total) AS max_confirmed
    FROM per_ip
    WHERE confirmed_total > 0
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      COALESCE(p.visible_total,         0) AS visible_total,
      COALESCE(p.confirmed_total,       0) AS confirmed_total,
      COALESCE(p.visible_subclasses,    0) AS visible_subclasses,
      COALESCE(p.visible_spells,        0) AS visible_spells,
      COALESCE(p.visible_monsters,      0) AS visible_monsters,
      COALESCE(p.visible_magic_items,   0) AS visible_magic_items,
      COALESCE(p.visible_species,       0) AS visible_species,
      COALESCE(p.confirmed_subclasses,  0) AS confirmed_subclasses,
      COALESCE(p.confirmed_spells,      0) AS confirmed_spells,
      COALESCE(p.confirmed_monsters,    0) AS confirmed_monsters,
      COALESCE(p.confirmed_magic_items, 0) AS confirmed_magic_items,
      COALESCE(p.confirmed_species,     0) AS confirmed_species,
      COALESCE(p.sections_captured,     0) AS sections_captured,
      lc.last_captured_at,
      tci.top_item
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip p USING (ip_name)
    LEFT JOIN top_confirmed_item tci USING (ip_name)
    LEFT JOIN last_capture lc USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  CASE
    WHEN j.confirmed_total = 0 THEN NULL
    ELSE ROUND(
      SAFE_DIVIDE(
        LOG10(j.confirmed_total + 1),
        LOG10((SELECT max_confirmed FROM max_total) + 1)
      ),
      4
    )
  END AS ddb_homebrew_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN j.sections_captured = 0 THEN 'no_ddb_data'
    WHEN j.confirmed_total = 0 THEN 'no_confirmed_ddb_signal'
    ELSE 'sufficient'
  END AS ddb_homebrew_status,

  CASE
    WHEN j.confirmed_total = 0 THEN 'NONE'
    WHEN j.confirmed_total <= 3 THEN 'LOW'
    WHEN j.confirmed_total <= 15 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS ddb_homebrew_signal_confidence,

  -- ─── DISAMBIGUATION FUNNEL ────────────────────────────────────────────
  -- Stage progression: visible -> confirmed (post AI Bouncer)
  j.visible_total                AS ddb_visible_total,
  j.confirmed_total              AS ddb_total_items,         -- backwards-compat name
  j.confirmed_total              AS ddb_confirmed_total,

  -- Per-section confirmed breakdown (the score input)
  j.confirmed_subclasses         AS ddb_subclasses_items,    -- backwards-compat
  j.confirmed_spells             AS ddb_spells_items,
  j.confirmed_monsters           AS ddb_monsters_items,
  j.confirmed_magic_items        AS ddb_magic_items_items,
  j.confirmed_species            AS ddb_species_items,

  -- Per-section visible-vs-confirmed (transparency on the AI Bouncer's drop rate)
  j.visible_subclasses           AS ddb_visible_subclasses,
  j.visible_spells               AS ddb_visible_spells,
  j.visible_monsters             AS ddb_visible_monsters,
  j.visible_magic_items          AS ddb_visible_magic_items,
  j.visible_species              AS ddb_visible_species,

  j.sections_captured            AS ddb_sections_captured,

  -- Top single CONFIRMED item across all sections — anchor homebrew per IP
  j.top_item.name                AS ddb_top_item_name,
  j.top_item.url                 AS ddb_top_item_url,
  j.top_item.ddb_section         AS ddb_top_item_section,
  j.top_item.adds                AS ddb_top_item_adds,

  j.last_captured_at AS ddb_last_captured_at,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN j.sections_captured = 0 THEN
      'Not yet captured on D&D Beyond.'
    WHEN j.confirmed_total = 0 AND j.visible_total = 0 THEN
      'No DDB homebrew exists for this IP across the 5 priority sections.'
    WHEN j.confirmed_total = 0 THEN
      CONCAT(
        'No CONFIRMED IP-specific homebrew. Captured ',
        CAST(j.visible_total AS STRING),
        ' items across ', CAST(j.sections_captured AS STRING),
        ' sections, but the AI Bouncer marked all as NOT genuinely about ',
        'this IP (likely fuzzy filter matches on tags/descriptions).'
      )
    ELSE
      CONCAT(
        CAST(j.confirmed_total AS STRING),
        ' confirmed IP-specific homebrew item(s) ',
        '(of ', CAST(j.visible_total AS STRING), ' captured) across ',
        CAST(j.sections_captured AS STRING), ' sections. Confirmed: ',
        CAST(j.confirmed_subclasses AS STRING), ' subclasses / ',
        CAST(j.confirmed_spells AS STRING),     ' spells / ',
        CAST(j.confirmed_monsters AS STRING),   ' monsters / ',
        CAST(j.confirmed_magic_items AS STRING),' magic-items / ',
        CAST(j.confirmed_species AS STRING),    ' species. Top: "',
        SUBSTR(COALESCE(j.top_item.name, ''), 1, 60),
        IF(LENGTH(COALESCE(j.top_item.name, '')) > 60, '..."', '"'),
        ' (', CAST(COALESCE(j.top_item.adds, 0) AS STRING), ' adds, ',
        COALESCE(j.top_item.ddb_section, ''), ').'
      )
  END AS ddb_homebrew_reasoning,

  -- Standardized output contract
  'community_reception'              AS signal_type,
  'ddb_homebrew_disambiguated'       AS stream_name,
  CURRENT_DATE()                     AS snapshot_date

FROM joined j
ORDER BY ddb_homebrew_score DESC NULLS LAST;
