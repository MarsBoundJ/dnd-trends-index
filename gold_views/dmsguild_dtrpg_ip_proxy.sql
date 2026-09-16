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
-- where Cyberpunk RED holds 6 ADAMANTINE SKUs - the rarest medal on the
-- site, 0.2% of its catalogue - and Fallout RPG has 5 Platinum SKUs plus
-- an Adamantine one. Buyers will pay for licensed TTRPGs; the
-- platform that allows them captures the revenue."*
--
-- ─── SCORE FORMULA ───────────────────────────────────────────────────
--
-- Two components per IP:
--   (a) tier-weighted strength    — what's the highest medal tier
--                                    among confirmed products?
--   (b) breadth                    — how many confirmed products?
--
-- Tier weights. ADAMANTINE is the most prestigious medal, not Platinum -
-- DriveThruRPG's Metal Legend lists the levels ascending, and Adamantine is
-- held by 0.2% of the catalogue against Platinum's 1.78%:
--   Adamantine = 1.00   (0.2% of the catalogue)
--   Mithral    = 0.67   (0.38%)
--   Platinum   = 0.33   (1.78%)
--   (any other tier = NULL, i.e. ABSTAINS from the average - see below)
--
-- Only these three exist: metal.php has exactly three shelves, so a row
-- carrying Gold/Silver/Copper/Electrum is an artefact of the V9 capture bug
-- fixed in #144, which read a product's tier off a neighbouring product's
-- title. Such a row is UNMEASURED, not a zero, and is excluded from the
-- average rather than scored 0.
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
  --
  -- ADAMANTINE IS THE TOP MEDAL, NOT PLATINUM. DriveThruRPG's own Metal Legend
  -- lists the levels in ASCENDING order — Copper, Silver, Electrum, Gold,
  -- Platinum, Mithral, Adamantine — with Adamantine held by 0.2% of the
  -- catalogue and Copper by 12.34%. This table previously ranked Platinum 1.00
  -- and Adamantine 0.67, i.e. inverted, from April 2026 until Sep 16 2026: every
  -- tier-weighted IP score penalised the rarest sellers.
  --
  -- GOLD/SILVER/COPPER ARE ABSENT ON PURPOSE, not merely re-ranked. metal.php
  -- has exactly three shelves, so no capture can legitimately produce any other
  -- tier. Rows that carry one are artefacts of the V9 bug fixed in #144, which
  -- read a product's tier off a NEIGHBOURING PRODUCT'S TITLE — "Trophy Gold"
  -- made the next product Gold. That is 6,760 rows, 35% of this stream's entire
  -- history.
  --
  -- This CTE is LEFT JOINed, so omitting those tiers gives them a NULL weight
  -- and drops them from scoring WITHOUT mutating a single row. The rows stay
  -- intact and recoverable if their true shelf is ever established.
  tier_weights AS (
    SELECT 'Adamantine' AS tier, 1.00 AS w UNION ALL
    SELECT 'Mithral',    0.67 UNION ALL
    SELECT 'Platinum',   0.33
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
      -- NULL, not 0.0, when the tier is not a real shelf.
      --
      -- COALESCE(...,0.0) made an unplaceable product a ZERO-WEIGHT one, so it
      -- dragged down an average it should never have entered. With the artefact
      -- tiers removed from tier_weights that became severe: Cyberpunk 2077 has 6
      -- genuine Adamantine products and 19 artefact 'Silver' rows, which would
      -- score 6/25 = 0.24 instead of 1.00 — punishing the IP for a bug in our own
      -- capture. AVG() and MAX() both skip NULL, so an unknown shelf now abstains
      -- instead of voting zero. Same rule the DDB stream already follows: a
      -- failure is UNMEASURED, never a measured zero.
      tw.w AS tier_weight
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
      -- How many of the confirmed products actually sat on a shelf. The score is
      -- an average over THESE, so this says how much of it is evidenced.
      COUNTIF(f.tier_weight IS NOT NULL)                          AS tiered_count,
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
      COALESCE(p.tiered_count,             0) AS tiered_count,
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
    -- Confirmed products exist but none sat on a real shelf, so there is nothing
    -- to average. Abstain rather than emit a score built on no tier evidence.
    WHEN j.avg_tier_weight IS NULL THEN NULL
    ELSE LEAST(1.0, ROUND(
      j.avg_tier_weight + IF(j.any_licensed, 0.10, 0.0),
      4
    ))
  END AS catalog_proxy_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN j.confirmed_total = 0 THEN 'no_confirmed_signal'
    -- Distinct from the above: the products are there, but not one of them could
    -- be placed on a shelf. Calling that 'sufficient' next to a NULL score would
    -- be the same lie as reading an unmeasured DDB combo as a zero.
    WHEN j.avg_tier_weight IS NULL THEN 'no_tier_signal'
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
  j.tiered_count,
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
