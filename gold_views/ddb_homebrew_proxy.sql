-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.ddb_homebrew_proxy — Stage 6a of community_reception (v1)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "DDB-native homebrew artifact intensity" scored from the
-- bookmarklet-captured rows in dnd_trends_raw.ddb_homebrew_counts.
--
-- D&D Beyond is the native D&D ecosystem — homebrew here is the
-- strongest "I want to play this IP at the table TODAY" signal in the
-- matrix. Per the Stage 6 plan, all 3 reviewer tools (ChatGPT / Gemini
-- / Perplexity) rated DDB Homebrew the #1 priority enrichment.
--
-- ─── INPUT ─────────────────────────────────────────────────────────────
--
-- ddb_homebrew_counts: one row per (ip_name, ddb_section) capture.
-- 5 priority sections in v1: subclasses, spells, monsters, magic-items,
-- species. Each row carries top_items[] (up to 30) with name, slug,
-- adds, views, comments, rating per item.
--
-- v1 takes the latest row per (ip_name, ddb_section) — re-captures
-- replace older captures via QUALIFY ROW_NUMBER().
--
-- ─── DISAMBIGUATION (DEFERRED TO v2) ───────────────────────────────────
--
-- v1 ships RAW counts. DDB's filter-name / filter-search params do
-- fuzzy matching across name + tags + description, so some "Hades"
-- captures land on a generic "Demigod" species (4440 adds) that's
-- universally popular rather than Hades-specific. Same kind of noise
-- that hit Stage 6b v0.
--
-- v2 (Stage 6c follow-up) will add an AI Bouncer pass:
-- classify_ddb_homebrew_results.py — Gemini Flash binary is_about_ip
-- per item, mirroring classify_external_homebrew_results.py. The gold
-- view will then score from confirmed_count instead of raw count.
--
-- For v1, the data trail (top_items per section, with adds counts)
-- makes the noise visible to anyone querying the table — reviewers
-- can see for themselves which captures are clean and which include
-- generic items.
--
-- ─── SCORE FORMULA ─────────────────────────────────────────────────────
--
-- Aggregate visible_items_count across all 5 priority sections per IP.
-- Log-normalize against the dataset-wide max:
--
--   ddb_homebrew_score = LOG10(total_items + 1) / LOG10(MAX_total + 1)
--
-- Same heavy-tailed pattern as Stage 6b external_homebrew. An IP with
-- 80 items across all sections gets ~1.0; an IP with 5 items gets ~0.4;
-- an IP with 0 items gets NULL (abstention).
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
--   total_items = 0   →  NULL with status='no_ddb_homebrew_signal'
--   1-5 items         →  scored, confidence='LOW'
--   6-25 items        →  scored, confidence='MEDIUM'
--   26+ items         →  scored, confidence='HIGH'
--
-- ─── DATA TRAIL ───────────────────────────────────────────────────────
--
-- Surfaces per-section item counts + the single top item across all
-- sections (highest adds count) for the demo "show me the top
-- homebrew for IP X on D&D Beyond" interaction.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.ddb_homebrew_proxy` AS

WITH

  latest_per_ip_section AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.ddb_homebrew_counts`
    -- Apr 29 evening: filter out the contaminated rows from the first
    -- bulk run (filter-name was used for /spells, /monsters,
    -- /magic-items where it doesn't actually filter; those rows have
    -- the global top items, not IP-filtered). Will be DELETE'd once
    -- the streaming buffer flushes.
    WHERE NOT (
      scraped_by = 'ddb_homebrew_bookmarklet_bulk'
      AND ddb_section IN ('spells', 'monsters', 'magic-items')
      AND scraped_at < TIMESTAMP('2026-04-29T23:00:00Z')
    )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, ddb_section ORDER BY scraped_at DESC
    ) = 1
  ),

  -- Aggregate per IP across all 5 priority sections
  per_ip AS (
    SELECT
      ip_name,
      SUM(visible_items_count) AS total_items,
      SUM(IF(ddb_section = 'subclasses',  visible_items_count, 0)) AS subclasses_items,
      SUM(IF(ddb_section = 'spells',      visible_items_count, 0)) AS spells_items,
      SUM(IF(ddb_section = 'monsters',    visible_items_count, 0)) AS monsters_items,
      SUM(IF(ddb_section = 'magic-items', visible_items_count, 0)) AS magic_items_items,
      SUM(IF(ddb_section = 'species',     visible_items_count, 0)) AS species_items,
      COUNT(DISTINCT ddb_section) AS sections_captured,
      MAX(scraped_at) AS last_captured_at
    FROM latest_per_ip_section
    WHERE ddb_section IN ('subclasses','spells','monsters','magic-items','species')
    GROUP BY ip_name
  ),

  -- Find the single top item across all sections (highest adds) for the trail
  flattened_items AS (
    SELECT l.ip_name, l.ddb_section, t.name, t.slug, t.url, t.adds
    FROM latest_per_ip_section l, UNNEST(l.top_items) AS t
    WHERE l.ddb_section IN ('subclasses','spells','monsters','magic-items','species')
  ),
  top_item_per_ip AS (
    SELECT
      ip_name,
      ARRAY_AGG(STRUCT(name, slug, url, ddb_section, adds) ORDER BY adds DESC LIMIT 1)[OFFSET(0)] AS top_item
    FROM flattened_items
    GROUP BY ip_name
  ),

  max_total AS (
    SELECT MAX(total_items) AS max_items
    FROM per_ip
    WHERE total_items > 0
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      COALESCE(p.total_items,         0) AS total_items,
      COALESCE(p.subclasses_items,    0) AS subclasses_items,
      COALESCE(p.spells_items,        0) AS spells_items,
      COALESCE(p.monsters_items,      0) AS monsters_items,
      COALESCE(p.magic_items_items,   0) AS magic_items_items,
      COALESCE(p.species_items,       0) AS species_items,
      COALESCE(p.sections_captured,   0) AS sections_captured,
      p.last_captured_at,
      ti.top_item
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip p USING (ip_name)
    LEFT JOIN top_item_per_ip ti USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  CASE
    WHEN j.total_items = 0 THEN NULL
    ELSE ROUND(
      SAFE_DIVIDE(
        LOG10(j.total_items + 1),
        LOG10((SELECT max_items FROM max_total) + 1)
      ),
      4
    )
  END AS ddb_homebrew_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN j.total_items = 0 AND j.sections_captured = 0 THEN 'no_ddb_data'
    WHEN j.total_items = 0 THEN 'no_ddb_homebrew_signal'
    ELSE 'sufficient'
  END AS ddb_homebrew_status,

  CASE
    WHEN j.total_items = 0 THEN 'NONE'
    WHEN j.total_items <= 5 THEN 'LOW'
    WHEN j.total_items <= 25 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS ddb_homebrew_signal_confidence,

  -- ─── PER-SECTION BREAKDOWN ────────────────────────────────────────────
  j.total_items                 AS ddb_total_items,
  j.subclasses_items            AS ddb_subclasses_items,
  j.spells_items                AS ddb_spells_items,
  j.monsters_items              AS ddb_monsters_items,
  j.magic_items_items           AS ddb_magic_items_items,
  j.species_items               AS ddb_species_items,
  j.sections_captured           AS ddb_sections_captured,

  -- Top single item across all sections — strongest "anchor" homebrew per IP
  j.top_item.name               AS ddb_top_item_name,
  j.top_item.url                AS ddb_top_item_url,
  j.top_item.ddb_section        AS ddb_top_item_section,
  j.top_item.adds               AS ddb_top_item_adds,

  j.last_captured_at AS ddb_last_captured_at,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN j.sections_captured = 0 THEN
      'Not yet captured on D&D Beyond.'
    WHEN j.total_items = 0 THEN
      'No DDB homebrew exists for this IP across the 5 priority sections.'
    ELSE
      CONCAT(
        CAST(j.total_items AS STRING),
        ' DDB homebrew item(s) across ',
        CAST(j.sections_captured AS STRING),
        ' sections (',
        CAST(j.subclasses_items AS STRING), ' subclasses / ',
        CAST(j.spells_items AS STRING),     ' spells / ',
        CAST(j.monsters_items AS STRING),   ' monsters / ',
        CAST(j.magic_items_items AS STRING),' magic-items / ',
        CAST(j.species_items AS STRING),    ' species). Top: "',
        SUBSTR(COALESCE(j.top_item.name, ''), 1, 60),
        IF(LENGTH(COALESCE(j.top_item.name, '')) > 60, '..."', '"'),
        ' (', CAST(COALESCE(j.top_item.adds, 0) AS STRING), ' adds, ',
        COALESCE(j.top_item.ddb_section, ''), ').'
      )
  END AS ddb_homebrew_reasoning,

  -- Standardized output contract
  'community_reception'      AS signal_type,
  'ddb_homebrew_bookmarklet' AS stream_name,
  CURRENT_DATE()             AS snapshot_date

FROM joined j
ORDER BY ddb_homebrew_score DESC NULLS LAST;
