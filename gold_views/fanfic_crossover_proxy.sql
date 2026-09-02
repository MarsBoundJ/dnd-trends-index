-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.fanfic_crossover_proxy — Stage 4 of community_reception
-- ═══════════════════════════════════════════════════════════════════════
--
-- Aggregates D&D-crossover counts captured manually via bookmarklets
-- (AO3 + FFN + future Wattpad) into a per-IP fanfic_proxy_score.
--
-- The premise: organic fanfiction crossover counts are revealed
-- preference at the deepest level. Fans don't write 4,500 Percy Jackson
-- × D&D crossover stories for marketing reasons — they write them
-- because the crossover already exists in the imagination. If the data
-- shows it, the demand is real.
--
-- ─── DATA FLOW ──────────────────────────────────────────────────────────
--
-- Phil/curator browses to AO3 / FFN search results page in their normal
-- browser, clicks the platform-specific bookmarklet, the bookmarklet
-- reads the visible work count from the DOM and POSTs to the bouncer
-- endpoint /system/fanfic/ingest-crossover-count, which writes to:
--
--   dnd_trends_raw.fanfic_crossover_counts
--     ip_name, platform, platform_canonical, work_count,
--     scraped_at, scraped_by, source_url
--
-- This view aggregates those rows into a per-IP composite score.
--
-- ─── SCORE FORMULA ──────────────────────────────────────────────────────
--
-- Per-platform normalization first (work counts span orders of
-- magnitude — LotR has thousands of crossovers, niche IPs have
-- single-digit), then average across available platforms with per-IP
-- renormalization (same pattern as PR #65).
--
-- Within each platform:
--   platform_score = log10(work_count + 1) / log10(MAX(work_count + 1) per platform)
--
-- This puts each platform's distribution on a [0, 1] scale where the
-- highest-count IP on that platform = 1.0 and zero counts = 0.0.
-- Logarithmic so that going from 10 to 100 fic counts roughly the
-- same as going from 100 to 1000 — fanfic distributions are heavy-
-- tailed.
--
-- Across platforms (for a given IP):
--   fanfic_proxy_score = AVG(platform_score across present platforms)
--
-- An IP captured on AO3 only gets the AO3-only score. An IP captured on
-- both AO3 + FFN gets the average. Future Wattpad data slots in
-- automatically.
--
-- ─── DEDUPLICATION ──────────────────────────────────────────────────────
--
-- Phil might click the bookmarklet multiple times for the same IP on
-- the same platform (e.g., re-capturing after the count grew, or fixing
-- a misclick). Take the LATEST scraped_at row per (ip_name, platform).
--
-- ─── CONFIDENCE TIER ────────────────────────────────────────────────────
--
-- Reflects how many platforms the IP has data on:
--   HIGH:    3 platforms present (AO3 + FFN + Wattpad)
--   MEDIUM:  2 platforms present
--   LOW:     1 platform present
--   NONE:    0 platforms (IP never captured — fanfic_proxy_score = NULL)
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.fanfic_crossover_proxy` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Latest snapshot per (ip_name, platform): if Phil captured the same
  -- IP × platform pair multiple times (refresh, misclick fix), keep
  -- the most recent one only.
  -- ────────────────────────────────────────────────────────────────────
  latest_per_pair AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.fanfic_crossover_counts`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name, platform ORDER BY scraped_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-platform normalization: log-scaled within each platform's
  -- distribution. Adding 1 to work_count avoids log(0) edge case.
  -- ────────────────────────────────────────────────────────────────────
  per_platform_normalized AS (
    SELECT
      ip_name,
      platform,
      platform_canonical,
      work_count,
      source_url,
      scraped_at,
      SAFE_DIVIDE(
        LOG10(work_count + 1),
        NULLIF(MAX(LOG10(work_count + 1)) OVER (PARTITION BY platform), 0)
      ) AS platform_score
    FROM latest_per_pair
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-IP aggregation across platforms — SCORE COMPUTATION.
  --
  -- WHERE platform != 'ffn': FFN excluded from the score (Apr 28, 2026).
  -- Rationale: when Phil first captured FFN data via the index page
  -- bookmarklet, only 6 of 142 IPs had ANY FFN crossover with D&D, and
  -- their counts maxed out at 7. Compared to AO3's max of 47,660 works
  -- on a single IP, that's a 6,800:1 ratio — too sparse for meaningful
  -- triangulation. Worse, the per-platform log-scale normalization
  -- gives the IP with 7 FFN works a platform_score of 1.0 (because 7
  -- IS the FFN dataset max), which would falsely boost any IP captured
  -- on both AO3 and FFN.
  --
  -- ⚠️ CORRECTION (Sep 2, 2026) — the 6,800:1 figure above is WRONG.
  -- That "AO3 max of 47,660" was Baldur's Gate 3, and it was never a
  -- crossover count: AO3 wrangles Baldur's Gate (Video Games) under the
  -- Dungeons & Dragons (Roleplaying Game) metatag, so the D&D x BG3
  -- filter returned the entire BG3 fandom. The row is now quarantined in
  -- dnd_trends_raw.fanfic_crossover_quarantine.
  --
  -- Against the true AO3 max of 84 (The Lord of the Rings), the real
  -- ratio on the two IPs carrying both signals is ~10:1 — LotR 84:7 and
  -- Doctor Who 13:2. That is an ordinary cross-platform scale gap, well
  -- within triangulation range.
  --
  -- The SECOND argument still stands and is why this exclusion is left
  -- in place for now: sparsity (6 of 142 IPs), plus the normalization
  -- pathology described above, which is genuinely independent of AO3's
  -- max. Note it is the same normalize-by-dataset-max fragility that let
  -- BG3 distort AO3 — approached from the other end (one absurdly large
  -- max there, a tiny noisy one here).
  --
  -- Revisiting the exclusion is work item E in
  -- docs/data_capture_hardening_plan.md. Likely reframe: use FFN as a
  -- corroboration flag ("does D&D crossover fic exist here at all?")
  -- rather than a scored magnitude — robust at small N, and nothing gets
  -- divided by a noisy max. NO BEHAVIOUR CHANGE in this edit.
  --
  -- The captured FFN rows stay in dnd_trends_raw.fanfic_crossover_counts
  -- for historical record + data-trail visibility (see per_platform_pivot
  -- below — ffn_work_count is still surfaced in the output schema). They
  -- just don't contribute to fanfic_proxy_score.
  --
  -- Same exclusion applies to Wattpad (no captures planned). The
  -- composite community_reception_score will be 4-source not 5-source:
  -- Gemini + MTG/D&D precedents + BGG + AO3 + Reddit (Stage 5).
  -- ────────────────────────────────────────────────────────────────────
  per_ip_aggregated AS (
    SELECT
      ip_name,
      ROUND(AVG(platform_score), 4) AS fanfic_proxy_score,
      COUNT(DISTINCT platform) AS platforms_present,
      STRING_AGG(platform, ',' ORDER BY platform) AS platforms_list,
      SUM(work_count) AS total_works_across_platforms,
      MAX(work_count) AS max_works_single_platform,
      MAX(scraped_at) AS latest_scrape
    FROM per_platform_normalized
    -- Allow-list of platforms that contribute to the score. AO3 only for
    -- now (FFN/Wattpad excluded — see comment above for rationale). To
    -- bring a new platform into the score, add it here AND verify the
    -- platform has comparable scale + meaningful coverage first.
    WHERE platform IN ('ao3')
    GROUP BY ip_name
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-platform breakdown: keep individual platform scores in the
  -- output for the data trail (so the UI can show AO3 vs FFN side-by-
  -- side and the WotC reviewer can see, e.g., "this IP has huge AO3
  -- presence but FFN is thin — different communities reading the same
  -- IP through different lenses").
  -- ────────────────────────────────────────────────────────────────────
  per_platform_pivot AS (
    SELECT
      ip_name,
      MAX(IF(platform = 'ao3', work_count, NULL)) AS ao3_work_count,
      MAX(IF(platform = 'ao3', platform_score, NULL)) AS ao3_platform_score,
      MAX(IF(platform = 'ao3', platform_canonical, NULL)) AS ao3_canonical_tag,
      MAX(IF(platform = 'ffn', work_count, NULL)) AS ffn_work_count,
      MAX(IF(platform = 'ffn', platform_score, NULL)) AS ffn_platform_score,
      MAX(IF(platform = 'wattpad', work_count, NULL)) AS wattpad_work_count,
      MAX(IF(platform = 'wattpad', platform_score, NULL)) AS wattpad_platform_score
    FROM per_platform_normalized
    GROUP BY ip_name
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Bring in the seed list so unmapped IPs surface as NULL rows.
  -- ────────────────────────────────────────────────────────────────────
  ub_with_fanfic AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      s.disambiguation,
      a.fanfic_proxy_score,
      a.platforms_present,
      a.platforms_list,
      a.total_works_across_platforms,
      a.max_works_single_platform,
      a.latest_scrape,
      p.ao3_work_count,
      ROUND(p.ao3_platform_score, 4) AS ao3_score,
      p.ao3_canonical_tag,
      p.ffn_work_count,
      ROUND(p.ffn_platform_score, 4) AS ffn_score,
      p.wattpad_work_count,
      ROUND(p.wattpad_platform_score, 4) AS wattpad_score
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip_aggregated a ON a.ip_name = s.ip_name
    LEFT JOIN per_platform_pivot p ON p.ip_name = s.ip_name
  )

SELECT
  u.ip_name,
  u.medium,
  u.tier,

  u.fanfic_proxy_score,

  -- Confidence tier from data: how many platforms have signal
  CASE
    WHEN u.platforms_present IS NULL OR u.platforms_present = 0 THEN 'NONE'
    WHEN u.platforms_present = 1 THEN 'LOW'
    WHEN u.platforms_present = 2 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS fanfic_signal_confidence,

  u.platforms_present,
  u.platforms_list,
  u.total_works_across_platforms,
  u.max_works_single_platform,

  -- Per-platform breakdown (for the data trail / UI display)
  u.ao3_work_count,
  u.ao3_score,
  u.ao3_canonical_tag,
  u.ffn_work_count,
  u.ffn_score,
  u.wattpad_work_count,
  u.wattpad_score,

  -- Human-readable reasoning string for the data trail
  CASE
    WHEN u.platforms_present IS NULL OR u.platforms_present = 0 THEN
      'No fanfic crossover data captured for this IP yet.'
    WHEN u.platforms_present = 1 THEN
      CONCAT(
        'Fanfic signal from 1 platform (', u.platforms_list, '): ',
        CAST(u.max_works_single_platform AS STRING), ' works.'
      )
    ELSE
      CONCAT(
        'Fanfic signal from ', CAST(u.platforms_present AS STRING),
        ' platforms (', u.platforms_list, '): ',
        CAST(u.total_works_across_platforms AS STRING), ' total works ',
        '(max ', CAST(u.max_works_single_platform AS STRING),
        ' on a single platform).'
      )
  END AS fanfic_proxy_reasoning,

  u.latest_scrape,

  -- Standardized output contract
  'community_reception' AS signal_type,
  'fanfic_crossover_proxy' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM ub_with_fanfic u
ORDER BY u.fanfic_proxy_score DESC NULLS LAST;
