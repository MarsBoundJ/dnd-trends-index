-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.fanfic_crossover_rate  —  work item C
--
-- The D&D-affinity ratio: crossover works / fandom total. Answers a different
-- question from the absolute count, and the two disagree sharply:
--
--     BY ABSOLUTE COUNT          BY RATE
--     1  LotR             84     1  Dark Souls      0.38%   (13 works)
--     2  Stranger Things  83     2  Cyberpunk 2077  0.37%
--     3  Avatar           60     3  Elden Ring      0.37%
--     ...                        ...
--        Attack on Titan  15        Attack on Titan 0.016%  (LAST)
--
-- Stranger Things is 2nd by volume and 15th by rate. Attack on Titan has more
-- crossover works than Dark Souls and the LOWEST conversion rate in the set.
-- Ranking on either axis alone tells a materially different story, which is
-- why both are reported here and neither is called "the" score.
--
-- ── THESE ARE CENSUSES, NOT SAMPLES ───────────────────────────────────
-- AO3 reports exactly how many works carry both tags. There is no sampling
-- error and the rate is EXACT as of fetch time. So the small-numerator problem
-- is not statistical uncertainty — it is FRAGILITY. Mistborn's rate rests on
-- 2 works; two authors could double it next month. rate_evidence_tier measures
-- how much a value depends on a handful of individual decisions, NOT how
-- confident we are that it is correct today.
--
-- Naming is deliberate: fanfic_crossover_proxy already exposes
-- `fanfic_signal_confidence`, which means something entirely different
-- (how many PLATFORMS captured this IP). Two unrelated ideas were heading for
-- the same word. A column whose name hid what it measured is exactly how
-- platforms_present reported a constant for five months.
--
-- ── WHY NOT SHRINK BY DEFAULT ─────────────────────────────────────────
-- The plan (docs/data_capture_hardening_plan.md item C) proposed shrinking
-- rates toward the global mean. Building it against real data, that is the
-- wrong default: shrinkage penalises high-rate/small-fandom IPs hardest, and
-- that combination is precisely the SLEEPER signal the breakdowns were built
-- to surface — ORV at 13/1,316 = 0.99% was the headline finding of the IP
-- licensing report. Shrinking it toward ~0.06% would erase the thing it was
-- valued for.
--
-- So: `crossover_rate` is raw and is the primary. `crossover_rate_shrunk` is
-- provided as a SECONDARY ordering for contexts that need small-k damped, and
-- is explicitly not the default. Rank within an evidence tier rather than
-- shrinking across tiers.
--
-- Scope: AO3 only. FFN carries no fandom totals and is excluded from scoring
-- (see work item E).
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.fanfic_crossover_rate` AS

WITH latest_capture AS (
  SELECT ip_name, work_count AS crossover_works, platform_canonical, scraped_at
  FROM `dnd-trends-index.dnd_trends_raw.fanfic_crossover_counts`
  WHERE platform = 'ao3'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ip_name ORDER BY scraped_at DESC) = 1
),

fandom_totals AS (
  -- One row per fandom from the newest census snapshot. A fandom is listed
  -- under several media categories with the same count.
  SELECT fandom, work_count AS fandom_total, is_umbrella, fetch_date
  FROM `dnd-trends-index.dnd_trends_raw.ao3_fandom_totals`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY fandom ORDER BY fetch_date DESC, work_count DESC
  ) = 1
),

joined AS (
  SELECT
    c.ip_name,
    c.platform_canonical AS ao3_tag,
    c.crossover_works,
    t.fandom_total,
    t.is_umbrella AS measured_at_umbrella_level,
    c.scraped_at AS crossover_captured_at,
    t.fetch_date AS fandom_total_as_of
  FROM latest_capture c
  LEFT JOIN fandom_totals t ON t.fandom = c.platform_canonical
),

-- Population rate across every IP with both halves. This is the prior the
-- shrunk variant pulls toward, and a useful reference line in its own right.
population AS (
  SELECT SAFE_DIVIDE(SUM(crossover_works), SUM(fandom_total)) AS global_rate
  FROM joined
  WHERE fandom_total IS NOT NULL AND fandom_total > 0
),

scored AS (
  SELECT
    j.*,
    p.global_rate,
    SAFE_DIVIDE(j.crossover_works, j.fandom_total) AS crossover_rate,
    -- Additive smoothing toward the population rate with a fixed pseudo-count.
    -- PSEUDO_N = 2000 damps the smallest fandoms without flattening the
    -- sleeper pattern: Mistborn (2/746) moves 0.268% -> ~0.12%, while
    -- LotR (84/53,522) is essentially unchanged.
    SAFE_DIVIDE(j.crossover_works + 2000 * p.global_rate,
                j.fandom_total + 2000) AS crossover_rate_shrunk,
    -- Fragility, not statistical confidence. See header.
    CASE
      WHEN j.crossover_works IS NULL OR j.fandom_total IS NULL THEN 'NONE'
      WHEN j.crossover_works >= 25 THEN 'HIGH'
      WHEN j.crossover_works >= 9  THEN 'MEDIUM'
      ELSE 'LOW'
    END AS rate_evidence_tier
  FROM joined j CROSS JOIN population p
)

SELECT
  ip_name,
  ao3_tag,

  -- The two axes. Neither is "the" score.
  crossover_works,
  fandom_total,
  ROUND(crossover_rate * 100, 4)        AS crossover_rate_pct,
  ROUND(crossover_rate_shrunk * 100, 4) AS crossover_rate_shrunk_pct,

  rate_evidence_tier,
  ROUND(global_rate * 100, 4) AS population_rate_pct,

  -- Rank within tier: the intended way to use the rate. Comparing a LOW-tier
  -- rate against a HIGH-tier one compares a 2-work estimate with an 84-work
  -- one as though they carried equal weight.
  RANK() OVER (PARTITION BY rate_evidence_tier ORDER BY crossover_rate DESC)
    AS rate_rank_in_tier,
  RANK() OVER (ORDER BY crossover_works DESC) AS absolute_rank,
  RANK() OVER (ORDER BY crossover_rate DESC) AS rate_rank_overall,

  -- A rate at or near 100% is metatag inflation, not affinity — the BG3
  -- signature. gold_data.fanfic_capture_guard flags it CRITICAL; surfaced here
  -- so nobody reads such a row as a finding.
  crossover_rate >= 0.90 AS rate_implies_metatag_inflation,

  measured_at_umbrella_level,
  crossover_captured_at,
  fandom_total_as_of
FROM scored
WHERE fandom_total IS NOT NULL
ORDER BY crossover_rate DESC;
