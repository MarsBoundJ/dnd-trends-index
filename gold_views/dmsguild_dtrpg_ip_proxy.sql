-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.dmsguild_dtrpg_ip_proxy — Stage 8 commercial revealed preference
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "do TTRPG buyers pay for licensed crossovers of this IP?" score
-- derived from bestseller-tier products on DriveThruRPG and DMs Guild.
-- This is the strongest commercial revealed-preference signal we capture
-- — buyers voting with their wallets on licensed third-party-IP TTRPGs.
--
-- ─── DATA SOURCES ────────────────────────────────────────────────────
--
-- Layer 1 (alias-substring filter, in classify_catalog_supply_titles.py):
--   dnd_trends_raw.catalog_supply         ← bookmarklet-harvested
--                                           bestseller pages, both sources
--
-- Layer 2 (Gemini Flash AI Bouncer, two axes):
--   dnd_trends_raw.catalog_supply_classified
--     - is_about_ip       — disambiguates alias false positives
--                           (e.g., "OVA: Anime RPG" → not Solo Leveling)
--     - is_licensed_ttrpg — distinguishes licensed commercial TTRPGs
--                           (Cyberpunk RED, Fallout RPG) from generic
--                           supplements that happened to alias-match
--
-- ─── WHY TWO PLATFORMS, ASYMMETRIC EXPECTATIONS ──────────────────────
--
-- DMs Guild and DriveThruRPG are both run by OneBookShelf but have
-- structurally different licensing rules:
--
--   DMs Guild     — D&D-exclusive content under WotC's community
--                   creator program. Creators CANNOT publish
--                   third-party-IP crossovers (no Stranger Things
--                   homebrew, no Cyberpunk content, etc.) due to
--                   licensing restrictions.
--
--   DriveThruRPG  — broader marketplace. Third-party publishers sell
--                   licensed IP TTRPGs (R. Talsorian's Cyberpunk RED,
--                   Modiphius's Fallout RPG, Free League's Aliens RPG,
--                   etc.) directly to TTRPG buyers.
--
-- Expected dataset shape (Apr 30 2026 build):
--   - DMs Guild  ~0 confirmed UB IPs (platform license rules)
--   - DriveThruRPG ~30-40 confirmed (Cyberpunk RED + Fallout RPG dominate)
--
-- The DMs Guild zero-state is itself a meaningful Hasbro-pitch finding:
-- *"Hasbro's own creator marketplace structurally cannot host the
-- IP-crossover content their players want. Compare to DriveThruRPG,
-- where Cyberpunk RED is a Platinum-tier bestseller and Fallout RPG
-- has 5 Platinum SKUs. Buyers will pay for licensed TTRPGs; the
-- platform that allows them captures the revenue."*
--
-- ─── SCORE FORMULA ───────────────────────────────────────────────────
--
-- Two components per IP:
--   (a) tier-weighted strength    — what's the highest medal tier
--                                    among confirmed products?
--   (b) breadth                    — how many confirmed products?
--
-- Tier weights (Platinum is the most prestigious bestseller medal):
--   Platinum   = 1.00
--   Mithral    = 0.83
--   Adamantine = 0.67
--   Gold       = 0.50
--   Silver     = 0.33
--   (other / missing = 0)
--
-- Final score = AVG(tier_weight) across confirmed products, with a
-- small +0.10 boost when ANY confirmed product is licensed_ttrpg=TRUE
-- (rewards platforms-with-licensed-deals over fan-content-only).
-- Capped at 1.0.
--
-- ─── ABSTENTION ──────────────────────────────────────────────────────
--
-- 0 confirmed products → status='no_confirmed_signal', score=NULL
-- 1-2 confirmed        → confidence=LOW
-- 3-5 confirmed        → confidence=MEDIUM
-- 6+ confirmed         → confidence=HIGH
--
-- Lower thresholds than community-sentiment proxies because each
-- catalog row represents real money changing hands — disproportionate
-- effort relative to a forum post.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.dmsguild_dtrpg_ip_proxy` AS

WITH

  -- Tier weights — keep aligned with analytics_dmsguild_dtrpg.sql.
  tier_weights AS (
    SELECT 'Platinum'   AS tier, 1.00 AS w UNION ALL
    SELECT 'Mithral',   0.83 UNION ALL
    SELECT 'Adamantine', 0.67 UNION ALL
    SELECT 'Gold',      0.50 UNION ALL
    SELECT 'Silver',    0.33
  ),

  -- Latest classification per (ip_name, source, title). The classifier
  -- keys uniquely on those three; same combo from a later --force run
  -- supersedes the earlier.
  classifications AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.catalog_supply_classified`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, source, title ORDER BY classified_at DESC
    ) = 1
  ),

  -- Confirmed-relevant products joined to tier weights.
  confirmed AS (
    SELECT
      c.ip_name,
      c.source,
      c.title,
      c.seller_tier,
      c.price,
      c.is_licensed_ttrpg,
      c.confidence AS classifier_confidence,
      COALESCE(tw.w, 0.0) AS tier_weight
    FROM classifications c
    LEFT JOIN tier_weights tw ON c.seller_tier = tw.tier
    WHERE c.is_about_ip = TRUE
  ),

  -- Per-IP aggregate. We surface per-source counts + the licensed-only
  -- count for the data trail.
  per_ip_agg AS (
    SELECT
      f.ip_name,
      COUNT(*)                                                    AS confirmed_total,
      COUNTIF(f.source = 'DMs Guild')                             AS dmsguild_confirmed_count,
      COUNTIF(f.source = 'DriveThruRPG')                          AS dtrpg_confirmed_count,
      COUNTIF(f.is_licensed_ttrpg)                                AS licensed_ttrpg_count,
      COUNTIF(f.source = 'DMs Guild' AND f.is_licensed_ttrpg)     AS dmsguild_licensed_count,
      COUNTIF(f.source = 'DriveThruRPG' AND f.is_licensed_ttrpg)  AS dtrpg_licensed_count,
      MAX(f.tier_weight)                                          AS top_tier_weight,
      AVG(f.tier_weight)                                          AS avg_tier_weight,
      LOGICAL_OR(f.is_licensed_ttrpg)                             AS any_licensed
    FROM confirmed f
    GROUP BY f.ip_name
  ),

  -- Sample top product (the strongest tier; tiebreak by licensed status).
  sample_top AS (
    SELECT
      f.ip_name,
      ARRAY_AGG(STRUCT(
        f.source AS source,
        f.title AS title,
        f.seller_tier AS seller_tier,
        f.price AS price,
        f.is_licensed_ttrpg AS is_licensed_ttrpg
      ) ORDER BY f.tier_weight DESC, f.is_licensed_ttrpg DESC LIMIT 1)
        [OFFSET(0)] AS top_product
    FROM confirmed f
    GROUP BY f.ip_name
  ),

  -- ub_candidate_seeds is the canonical 142-IP list. LEFT JOIN ensures
  -- every IP gets a row even if it has zero matches anywhere.
  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      COALESCE(p.confirmed_total,         0)  AS confirmed_total,
      COALESCE(p.dmsguild_confirmed_count, 0) AS dmsguild_confirmed_count,
      COALESCE(p.dtrpg_confirmed_count,    0) AS dtrpg_confirmed_count,
      COALESCE(p.licensed_ttrpg_count,     0) AS licensed_ttrpg_count,
      COALESCE(p.dmsguild_licensed_count,  0) AS dmsguild_licensed_count,
      COALESCE(p.dtrpg_licensed_count,     0) AS dtrpg_licensed_count,
      p.top_tier_weight,
      p.avg_tier_weight,
      COALESCE(p.any_licensed, FALSE)         AS any_licensed,
      st.top_product
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip_agg p USING (ip_name)
    LEFT JOIN sample_top st USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  -- AVG tier weight across confirmed products + 0.10 licensed bonus,
  -- capped at 1.0. NULL when no confirmed signal.
  CASE
    WHEN j.confirmed_total = 0 THEN NULL
    ELSE LEAST(1.0, ROUND(
      j.avg_tier_weight + IF(j.any_licensed, 0.10, 0.0),
      4
    ))
  END AS catalog_proxy_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN j.confirmed_total = 0 THEN 'no_confirmed_signal'
    ELSE 'sufficient'
  END AS catalog_status,

  CASE
    WHEN j.confirmed_total = 0 THEN 'NONE'
    WHEN j.confirmed_total <= 2 THEN 'LOW'
    WHEN j.confirmed_total <= 5 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS catalog_signal_confidence,

  -- ─── DATA TRAIL: per-source breakdown ─────────────────────────────────
  j.confirmed_total,
  j.dmsguild_confirmed_count,
  j.dtrpg_confirmed_count,
  j.licensed_ttrpg_count,
  j.dmsguild_licensed_count,
  j.dtrpg_licensed_count,
  ROUND(COALESCE(j.top_tier_weight, 0), 4) AS top_tier_weight,
  ROUND(COALESCE(j.avg_tier_weight, 0), 4) AS avg_tier_weight,
  j.any_licensed,

  -- ─── SAMPLE PRODUCT (data trail) ──────────────────────────────────────
  j.top_product.title       AS top_product_title,
  j.top_product.source      AS top_product_source,
  j.top_product.seller_tier AS top_product_tier,
  j.top_product.price       AS top_product_price,
  j.top_product.is_licensed_ttrpg AS top_product_is_licensed,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN j.confirmed_total = 0 THEN
      'No confirmed UB-IP products on DMs Guild or DriveThruRPG.'
    WHEN j.any_licensed AND j.dtrpg_licensed_count > 0 THEN
      CONCAT(
        CAST(j.confirmed_total AS STRING),
        ' confirmed product(s); ',
        CAST(j.licensed_ttrpg_count AS STRING),
        ' licensed TTRPG (DriveThruRPG ',
        CAST(j.dtrpg_licensed_count AS STRING),
        '). Top: ',
        SUBSTR(COALESCE(j.top_product.title, ''), 1, 60),
        ' [', COALESCE(j.top_product.seller_tier, ''), ']'
      )
    ELSE
      CONCAT(
        CAST(j.confirmed_total AS STRING),
        ' confirmed product(s) on DMs Guild ',
        CAST(j.dmsguild_confirmed_count AS STRING),
        ' / DriveThruRPG ',
        CAST(j.dtrpg_confirmed_count AS STRING),
        '. Top: ',
        SUBSTR(COALESCE(j.top_product.title, ''), 1, 60)
      )
  END AS catalog_reasoning,

  -- Standardized output contract
  'commercial_revealed_preference' AS signal_type,
  'dmsguild_dtrpg_ip'              AS stream_name,
  CURRENT_DATE()                    AS snapshot_date

FROM joined j
ORDER BY catalog_proxy_score DESC NULLS LAST, j.ip_name;
