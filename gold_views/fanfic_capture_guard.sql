-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.fanfic_capture_guard
--
-- Flags fanfic crossover captures that are probably wrong, BEFORE they reach
-- a report. Built Sep 2, 2026 after two live errors that were each detectable
-- from data already in the warehouse — and each survived four months because
-- nobody ran the comparison:
--
--   1. BG3 metatag inflation. fanfic_crossover_counts held 49,020 as a "D&D x
--      BG3 crossover count"; ao3_tag_counts held 48,997 as the size of the
--      whole BG3 fandom. Two tables in the same dataset, ~0.05% apart. The
--      "crossover" number was the entire fandom — AO3 wrangles Baldur's Gate
--      under the D&D metatag, so the filter returned everything.
--
--   2. One Piece stale tag. The gold table held 5 while
--      pitch/report/trusight_breakdowns_scratch.md held 46 — a 9x disagreement
--      that stood from May to September. Re-capture with the canonical tag
--      gives 52.
--
-- Neither was a collection failure. Both were CROSS-CHECKING failures.
--
-- Severity:
--   CRITICAL — do not use this number; near-certainly measuring the wrong thing
--   WARN     — plausible but unusual; verify the tag before relying on it
--   INFO     — hygiene
--
-- Coverage: the fandom-total join reads dnd_trends_raw.ao3_fandom_totals, the full
-- AO3 canonical census (~59k fandoms, loaded weekly by
-- cloud_functions/ao3_fandom_listing), falling back to ao3_tag_counts for tags the
-- listing does not carry. Before that table existed the inflation check resolved
-- for 1 of 26 AO3 IPs — a smoke detector wired to one room.
-- The magnitude and zero checks need no fandom totals and cover every IP regardless.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.fanfic_capture_guard` AS

WITH latest AS (
  -- One row per (ip_name, platform) — the capture a consumer would actually read.
  SELECT *
  FROM `dnd-trends-index.dnd_trends_raw.fanfic_crossover_counts`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ip_name, platform ORDER BY scraped_at DESC
  ) = 1
),

listing_totals AS (
  -- PRIMARY source: the full AO3 canonical fandom census (~59k fandoms),
  -- loaded weekly by cloud_functions/ao3_fandom_listing. A fandom is listed
  -- under several media categories with the same count, so take one row per
  -- fandom from the newest snapshot.
  SELECT fandom AS tag_name, work_count AS fandom_total
  FROM `dnd-trends-index.dnd_trends_raw.ao3_fandom_totals`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY fandom ORDER BY fetch_date DESC, work_count DESC) = 1
),

tag_totals AS (
  -- FALLBACK: the 23 D&D-native tags tracked by cloud_functions/ao3_harvester.
  -- Kept because it covers tags that are not fandoms in the listing sense, and
  -- because it is the independent source that corroborated the BG3 artifact.
  SELECT tag_name, work_count AS fandom_total
  FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`
  WHERE tag_type = 'fandom'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tag_name ORDER BY fetch_date DESC) = 1
),

fandom_totals AS (
  SELECT * FROM listing_totals
  UNION ALL
  SELECT t.* FROM tag_totals t
  WHERE NOT EXISTS (SELECT 1 FROM listing_totals l WHERE l.tag_name = t.tag_name)
),

platform_stats AS (
  -- Median is deliberate: the mean is destroyed by exactly the outlier we are
  -- hunting. BG3 at 49,020 would have dragged the AO3 mean to ~2,000 and hidden
  -- itself; the median stayed at 15.
  SELECT
    platform,
    APPROX_QUANTILES(work_count, 2)[OFFSET(1)] AS median_count,
    COUNT(*) AS ip_count
  FROM latest
  GROUP BY platform
),

enriched AS (
  SELECT
    l.ip_name,
    l.platform,
    l.work_count,
    l.platform_canonical,
    l.scraped_at,
    l.scraped_by,
    f.fandom_total,
    SAFE_DIVIDE(l.work_count, f.fandom_total) AS share_of_fandom,
    p.median_count,
    SAFE_DIVIDE(l.work_count, NULLIF(p.median_count, 0)) AS x_median,
    DATE_DIFF(CURRENT_DATE(), DATE(l.scraped_at), DAY) AS age_days
  FROM latest l
  LEFT JOIN fandom_totals f ON f.tag_name = l.platform_canonical
  LEFT JOIN platform_stats p ON p.platform = l.platform
),

findings AS (
  -- ── 1. Metatag inflation ────────────────────────────────────────────
  -- A crossover count that is ~all of the fandom is not a crossover count.
  SELECT *, 'METATAG_INFLATION' AS finding, 'CRITICAL' AS severity,
         FORMAT('%d works = %.1f%% of the %d-work fandom — the filter is returning the whole fandom, not an intersection',
                work_count, share_of_fandom * 100, fandom_total) AS detail
  FROM enriched WHERE share_of_fandom >= 0.90

  UNION ALL
  SELECT *, 'NEAR_FANDOM_TOTAL', 'WARN',
         FORMAT('%d works = %.1f%% of the %d-work fandom — implausibly high for a crossover; verify the tag',
                work_count, share_of_fandom * 100, fandom_total)
  FROM enriched WHERE share_of_fandom >= 0.50 AND share_of_fandom < 0.90

  -- ── 2. Zero counts ──────────────────────────────────────────────────
  -- Empirical, not theoretical: every zero ever recorded on AO3 turned out to be
  -- a stale tag or an unfilterable one. None was a measured zero.
  UNION ALL
  SELECT *, 'ZERO_COUNT', 'CRITICAL',
         'Zero is unverified until the tag is confirmed canonical. Every AO3 zero found so far was a bug, not a measurement.'
  FROM enriched WHERE work_count = 0

  -- ── 3. Magnitude outliers (needs no fandom total) ───────────────────
  -- This is the check that would have caught BG3 on day one with no join at all.
  UNION ALL
  SELECT *, 'EXTREME_OUTLIER', 'CRITICAL',
         FORMAT('%d works is %.0fx the %s median of %d — verify before use',
                work_count, x_median, platform, median_count)
  FROM enriched WHERE x_median >= 50

  UNION ALL
  SELECT *, 'HIGH_OUTLIER', 'WARN',
         FORMAT('%d works is %.1fx the %s median of %d',
                work_count, x_median, platform, median_count)
  FROM enriched WHERE x_median >= 10 AND x_median < 50

  -- ── 4. Hygiene ──────────────────────────────────────────────────────
  UNION ALL
  SELECT *, 'NO_FANDOM_TOTAL', 'INFO',
         'No fandom total available — the metatag-inflation check cannot run for this IP. Widens when the AO3 listing scrape lands.'
  FROM enriched WHERE fandom_total IS NULL AND platform = 'ao3'

  UNION ALL
  SELECT *, 'STALE_CAPTURE', 'INFO',
         FORMAT('Last captured %d days ago', age_days)
  FROM enriched WHERE age_days > 90
)

SELECT
  severity,
  finding,
  ip_name,
  platform,
  work_count,
  fandom_total,
  ROUND(share_of_fandom, 4) AS share_of_fandom,
  ROUND(x_median, 1) AS x_platform_median,
  platform_canonical,
  age_days,
  detail,
  scraped_by,
  scraped_at
FROM findings
ORDER BY
  CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
  work_count DESC;
