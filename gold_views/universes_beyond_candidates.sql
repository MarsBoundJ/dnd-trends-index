-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.universes_beyond_candidates — Step 9.9 UB Matrix ranking view
-- ═══════════════════════════════════════════════════════════════════════
--
-- Ranks non-D&D/non-MTG IPs by license-fit score for Universes Beyond
-- (MTG crossover) or D&D-setting licensing.  Source inputs:
--   * dnd_trends_raw.ub_candidate_seeds       — the 142-IP curated seed
--     list with medium / tier / steam_app_id / fandom_wiki_slug join keys
--   * dnd_trends_raw.ub_candidate_enrichment  — Gemini 2.5-flash rubric
--     scores (5 independent dims + confidence + reasoning)
--   * dnd_trends_raw.fandom_daily_metrics     — wiki view telemetry
--     (view_count, rank_position, hype_score)
--   * dnd_trends_raw.steam_player_counts      — Steam concurrent-player
--     snapshots for the 49 tracked apps
--
-- ─── COMPOSITE FORMULA (baseline, tunable at the constants block below) ─
--
--   license_fit_score =
--     0.60 × rubric_composite           -- primary, all 142 IPs covered
--   + 0.30 × fandom_heat_overlay        -- secondary, 24 IPs covered
--   + 0.10 × steam_velocity_overlay     -- tertiary, GATED on coverage
--
-- Rubric-primary by design.  The rubric is the intellectual core (5
-- calibrated dimensions, orthogonality anchors, Gemini-validated on all
-- 142 IPs).  Fandom + Steam overlays validate or challenge the rubric
-- where live market signals exist; they don't carry primary weight.
--
-- ─── STEAM GATE ─────────────────────────────────────────────────────────
--
-- Steam velocity requires multiple days of concurrent-player history per
-- app before the growth-rate math stabilizes.  As of the 9.9 launch day
-- (2026-04-20), the harvester just extended from 5 D&D games to 49 total
-- (5 D&D + 44 UB).  The 44 UB games have only ~1 day of history and
-- velocity will be unreliable for ~1 week.
--
-- The gate: if fewer than 20 IPs have computable Steam velocity, the
-- 0.10 steam weight redistributes proportionally into rubric (× 0.667)
-- and fandom (× 0.333), keeping composite sum = 1.0.  Once 20+ IPs have
-- enough Steam history, the gate flips automatically — no code change
-- needed, the CASE expression re-weights itself.
--
-- Future sessions reading this view should SEE the gate (not discover
-- it during debugging).  Tune the threshold or weights at the
-- `thresholds`/`weights` CTEs below — NOT in the final SELECT.
--
-- ─── FANDOM SIGNAL CAVEAT (harvester debt) ──────────────────────────────
--
-- fandom_daily_metrics.view_count has been 0 across ALL wikis for the
-- past 14+ days.  Fandom's /api/v1/Articles/Top no longer populates
-- the "views" field; only rank_position + hype_score are live.  This
-- view uses SUM(hype_score) per wiki_slug as the intensity proxy —
-- preserves within-wiki rank information, captures cross-wiki depth
-- (a wiki with 100 top articles contributes more than one with 11).
--
-- Tracked as harvester debt: project_data_quality_backlog.md
-- § "Fandom view_count extraction broken (Apr 20, 2026)".  When the
-- harvester is fixed, swap SUM(hype_score) for SUM(view_count) here
-- and update the header.
--
-- ─── FALLOUT DEDUP ──────────────────────────────────────────────────────
--
-- ub_candidate_enrichment has a stale tv_film "Fallout" row (BQ
-- streaming buffer prevented a DELETE at the time of the Fallout
-- consolidation fix).  QUALIFY ROW_NUMBER() takes the most-recently-
-- enriched row per ip_name; the stale row drops out.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.universes_beyond_candidates` AS

WITH
  -- ────────────────────────────────────────────────────────────────────
  -- Tuning constants — composite weights + gate threshold.
  -- Edit these to retune WITHOUT redeploying any code.
  -- ────────────────────────────────────────────────────────────────────
  thresholds AS (
    SELECT
      20 AS steam_coverage_floor,       -- gate flips on at 20 IPs
      7  AS fandom_window_days,         -- SUM hype_score over last N days
      3  AS steam_recent_days,          -- velocity: recent-3 vs prior-N
      4  AS steam_prior_days
  ),
  baseline_weights AS (
    SELECT
      0.60 AS w_rubric_full,
      0.30 AS w_fandom_full,
      0.10 AS w_steam_full,
      -- Gated weights (Steam OFF, redistribute): rubric + fandom scale
      -- proportionally from 0.60/0.30 to sum to 1.0.
      0.60 / 0.90 AS w_rubric_gated,    -- ≈ 0.667
      0.30 / 0.90 AS w_fandom_gated,    -- ≈ 0.333
      0.00        AS w_steam_gated
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Rubric: one row per IP, with stale-Fallout dedup.
  -- ────────────────────────────────────────────────────────────────────
  rubric_latest AS (
    SELECT
      ip_name,
      medium,
      tier,
      genre_fit,
      combat_translatability,
      party_dynamics_fit,
      setting_portability,
      fanbase_ttrpg_overlap,
      confidence AS rubric_confidence,
      reasoning  AS rubric_reasoning,
      (genre_fit + combat_translatability + party_dynamics_fit
       + setting_portability + fanbase_ttrpg_overlap) / 5.0 AS rubric_composite,
      enriched_at
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_enrichment`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name ORDER BY enriched_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Fandom heat: SUM(hype_score) across ALL articles per wiki over the
  -- last N days.  Preserves within-wiki rank intensity + cross-wiki
  -- depth.  Proxy for view_count pending harvester fix.
  -- ────────────────────────────────────────────────────────────────────
  fandom_agg AS (
    SELECT
      f.wiki_slug,
      SUM(f.hype_score) AS fandom_hype_sum,
      COUNT(*)          AS fandom_article_count
    FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics` f
    CROSS JOIN thresholds t
    WHERE f.extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL t.fandom_window_days DAY)
    GROUP BY f.wiki_slug
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Steam velocity: (recent_mean - prior_mean) / prior_mean.
  -- NULL when either window is empty (new apps, harvester outage).
  -- ────────────────────────────────────────────────────────────────────
  steam_recent AS (
    SELECT
      s.app_id,
      AVG(s.concurrent_players) AS recent_mean
    FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts` s
    CROSS JOIN thresholds t
    WHERE s.snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL t.steam_recent_days DAY)
    GROUP BY s.app_id
  ),
  steam_prior AS (
    SELECT
      s.app_id,
      AVG(s.concurrent_players) AS prior_mean
    FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts` s
    CROSS JOIN thresholds t
    WHERE s.snapshot_date BETWEEN
      DATE_SUB(CURRENT_DATE(), INTERVAL (t.steam_recent_days + t.steam_prior_days) DAY)
      AND DATE_SUB(CURRENT_DATE(), INTERVAL (t.steam_recent_days + 1) DAY)
    GROUP BY s.app_id
  ),
  steam_agg AS (
    SELECT
      r.app_id,
      r.recent_mean,
      p.prior_mean,
      SAFE_DIVIDE(r.recent_mean - p.prior_mean, NULLIF(p.prior_mean, 0))
        AS steam_velocity
    FROM steam_recent r
    LEFT JOIN steam_prior p USING (app_id)
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Join seed + signals.  LEFT JOINs keep 142 rows; unmatched signals
  -- land as NULL (the UI reads NULL as "coming soon / not yet covered").
  -- ────────────────────────────────────────────────────────────────────
  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      s.disambiguation,
      s.steam_app_id,
      s.fandom_wiki_slug,

      r.rubric_composite,
      r.genre_fit,
      r.combat_translatability,
      r.party_dynamics_fit,
      r.setting_portability,
      r.fanbase_ttrpg_overlap,
      r.rubric_confidence,
      r.rubric_reasoning,

      f.fandom_hype_sum,
      f.fandom_article_count,

      st.recent_mean     AS steam_recent_players,
      st.prior_mean      AS steam_prior_players,
      st.steam_velocity
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN rubric_latest r USING (ip_name)
    LEFT JOIN fandom_agg    f ON f.wiki_slug = s.fandom_wiki_slug
    LEFT JOIN steam_agg     st ON st.app_id  = s.steam_app_id
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Normalize each signal to [0,1] within the current dataset so the
  -- weighted sum is comparable.  rubric_composite is already [0,1].
  -- ────────────────────────────────────────────────────────────────────
  normalized AS (
    SELECT
      j.*,
      -- Fandom: normalize by max sum across all IPs with coverage.
      SAFE_DIVIDE(
        j.fandom_hype_sum,
        NULLIF(MAX(j.fandom_hype_sum) OVER (), 0)
      ) AS fandom_norm,
      -- Steam velocity: min-max normalize to [0,1] across IPs with
      -- computable velocity.  Strong-negative velocity → 0; strong-
      -- positive → 1.  NULL stays NULL.
      SAFE_DIVIDE(
        j.steam_velocity - MIN(j.steam_velocity) OVER (),
        NULLIF(MAX(j.steam_velocity) OVER () - MIN(j.steam_velocity) OVER (), 0)
      ) AS steam_norm
    FROM joined j
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Coverage gate: count IPs with computable Steam velocity.  If the
  -- count is below the threshold, steam weight collapses to 0 and
  -- rubric+fandom scale up proportionally.
  -- ────────────────────────────────────────────────────────────────────
  coverage AS (
    SELECT
      COUNTIF(steam_velocity IS NOT NULL) AS steam_covered_ips,
      COUNTIF(fandom_hype_sum IS NOT NULL) AS fandom_covered_ips,
      COUNTIF(rubric_composite IS NOT NULL) AS rubric_covered_ips,
      COUNT(*) AS total_ips
    FROM joined
  ),
  active_weights AS (
    SELECT
      CASE WHEN c.steam_covered_ips >= t.steam_coverage_floor
           THEN bw.w_rubric_full ELSE bw.w_rubric_gated END AS w_rubric,
      CASE WHEN c.steam_covered_ips >= t.steam_coverage_floor
           THEN bw.w_fandom_full ELSE bw.w_fandom_gated END AS w_fandom,
      CASE WHEN c.steam_covered_ips >= t.steam_coverage_floor
           THEN bw.w_steam_full  ELSE bw.w_steam_gated  END AS w_steam,
      (c.steam_covered_ips >= t.steam_coverage_floor) AS steam_gate_active,
      c.steam_covered_ips,
      c.fandom_covered_ips,
      c.rubric_covered_ips,
      c.total_ips
    FROM coverage c
    CROSS JOIN thresholds t
    CROSS JOIN baseline_weights bw
  )

-- ══════════════════════════════════════════════════════════════════════
-- Final SELECT — one row per IP, ranked by license_fit_score.
-- ══════════════════════════════════════════════════════════════════════
SELECT
  n.ip_name,
  n.medium,
  n.tier,
  n.disambiguation,

  -- Primary composite (rubric-weighted + overlays + gate math).
  -- NULLs are treated as 0 for the composite — an IP without Fandom
  -- signal doesn't get boosted; it just rides on the rubric.  The UI
  -- surfaces the underlying NULLs via the data-trail columns below.
  ROUND(
    w.w_rubric * COALESCE(n.rubric_composite, 0)
    + w.w_fandom * COALESCE(n.fandom_norm, 0)
    + w.w_steam  * COALESCE(n.steam_norm,  0),
    4
  ) AS license_fit_score,

  -- Rubric detail (all 142 IPs have these)
  n.rubric_composite,
  n.genre_fit,
  n.combat_translatability,
  n.party_dynamics_fit,
  n.setting_portability,
  n.fanbase_ttrpg_overlap,
  n.rubric_confidence,
  n.rubric_reasoning,

  -- Fandom detail (24 IPs)
  n.fandom_wiki_slug,
  n.fandom_hype_sum,
  n.fandom_article_count,
  n.fandom_norm,

  -- Steam detail (fills over next N days)
  n.steam_app_id,
  n.steam_recent_players,
  n.steam_prior_players,
  n.steam_velocity,
  n.steam_norm,

  -- Metadata: gate state + coverage + active weights.  Same values on
  -- every row (windowed), which lets the Bouncer endpoint lift them
  -- into a top-level status object without an extra query.
  w.steam_gate_active,
  w.steam_covered_ips,
  w.fandom_covered_ips,
  w.rubric_covered_ips,
  w.total_ips,
  w.w_rubric AS weight_rubric,
  w.w_fandom AS weight_fandom,
  w.w_steam  AS weight_steam

FROM normalized n
CROSS JOIN active_weights w
ORDER BY license_fit_score DESC NULLS LAST;
