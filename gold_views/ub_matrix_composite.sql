-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.ub_matrix_composite — Phase A composite for the UB Matrix
-- ═══════════════════════════════════════════════════════════════════════
--
-- Combines all five community_reception sub-scores into a per-IP
-- composite, plus exposes the reverse-funnel acquisition signal as a
-- separate matrix dimension. Per locked decision #1, reception and
-- acquisition are NEVER averaged together — they're orthogonal axes
-- on the matrix.
--
-- ─── DUAL-VIEW DESIGN ──────────────────────────────────────────────────
--
-- Outputs TWO composites per IP, after triangulating Gemini, Perplexity,
-- and ChatGPT input on the weighting question (see
-- docs/community_reception_findings.md):
--
--   composite_score_equal     — equal weights with per-IP renormalization
--   composite_score_weighted  — weighted by data-reliability hierarchy
--                               with per-IP renormalization
--
-- Weighted version uses ChatGPT-revised weights:
--   Precedents (Stage 2)        0.30  (D&D 0.70 / MTG 0.30 sub-weighting)
--   BGG proxy (Stage 3)         0.20
--   AO3 fanfic (Stage 4)        0.20
--   Reddit D&D (Stage 5 P1)     0.15
--   Gemini baseline (Stage 1)   0.15
--   Homebrew (Stage 6)          0.10  (NEW Apr 29 — relative to others
--                                       it's lowest because v1 coverage
--                                       is thin; per-IP renormalization
--                                       handles the unbalanced sum)
--   Forum presence (Stage 7)    0.10  (NEW Apr 29 — DM/whale demographic
--                                       brand-purity signal. v1 is
--                                       presence-only; sentiment in v2
--                                       would justify higher weight)
--
-- When the two scores AGREE (low divergence) → strong signal.
-- When they DIVERGE (>0.15) → flag for inspection. The divergence
-- itself is informative — it means weighted sources point a different
-- direction than equal-weighted, which is itself analytical content.
--
-- Per-IP renormalization (same as PR #65 Option A): if a source is
-- NULL for an IP, redistribute its weight proportionally across the
-- available sources so weights always sum to 1.0.
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
-- composite_score = NULL if measured_sources_count < 2 (counting
-- Stages 2-5 only; Stage 1 Gemini baseline doesn't count as "measured"
-- since it's modeled). Status flag indicates why.
--
--   measured_sources_count >= 2  →  composite scored, status='sufficient'
--   measured_sources_count = 1   →  composite NULL, status='thin_evidence'
--   measured_sources_count = 0   →  composite NULL,
--                                    status='modeled_only_low_confidence'
--                                    (Gemini baseline still surfaced as
--                                    gemini_baseline_score for context)
--
-- ─── CONSILIENCE METRICS ───────────────────────────────────────────────
--
-- Surface cross-source agreement as both a continuous score AND a
-- discrete badge:
--
--   sources_present     — count of all 5 sub-scores non-NULL (incl. Gemini)
--   sources_positive    — count of sub-scores >= 0.7
--   sources_negative    — count of sub-scores <= 0.3
--   consilience_score   — max(positive, negative) / sources_present, [0, 1]
--                          (1.0 = perfect agreement; 0.5 = no consensus)
--   highly_corroborated — TRUE if 3+ MEASURED sources (excluding Gemini)
--                          all within 0.2 of each other
--
-- ─── BCG QUADRANT (matrix dimension flags) ─────────────────────────────
--
-- For IPs where BOTH composite reception AND acquisition scores exist,
-- classify into the 4-quadrant licensing strategy framework (Gemini's
-- contribution):
--
--   gold_mine    (high reception × high acquisition)  Aggressive Core Integration
--   fan_service  (high reception × low  acquisition)  Retention Play / Limited Run
--   trojan_horse (low  reception × high acquisition)  Powered-by-5e Standalone
--   brand_hazard (low  reception × low  acquisition)  Hard Pass
--
-- Thresholds: HIGH = 0.65, LOW = 0.40. Mid-zone IPs get quadrant=NULL
-- ("mixed signal" — no confident classification).
--
-- Use composite_score_equal for the quadrant decision (less weighted
-- bias for the demo narrative). Weighted view available alongside.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.ub_matrix_composite` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Source 1: Gemini-anchored baseline (Stage 1)
  -- Always present for all 142 IPs.
  -- ────────────────────────────────────────────────────────────────────
  gemini_score AS (
    SELECT
      ip_name,
      ROUND(community_reception, 4) AS gemini_baseline_score
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_enrichment`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name ORDER BY enriched_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Source 2: Combined precedent score
  -- D&D crossover precedents weighted 0.70; MTG UB precedents weighted
  -- 0.30 when both present. (D&D community judging same product line is
  -- strictly stronger signal than adjacent-community precedent.)
  -- Currently only matches by exact ip_name; analogical matching is a
  -- future enhancement.
  -- ────────────────────────────────────────────────────────────────────
  dnd_precedent_per_ip AS (
    SELECT
      ip_name,
      AVG(reception_score) AS dnd_precedent_avg,
      COUNT(*) AS dnd_precedent_count
    FROM `dnd-trends-index.dnd_trends_raw.dnd_crossover_precedents`
    GROUP BY ip_name
  ),
  mtg_precedent_per_ip AS (
    SELECT
      ip_name,
      AVG(reception_score) AS mtg_precedent_avg,
      COUNT(*) AS mtg_precedent_count
    FROM `dnd-trends-index.dnd_trends_raw.ub_mtg_precedents`
    GROUP BY ip_name
  ),
  precedent_combined AS (
    SELECT
      COALESCE(d.ip_name, m.ip_name) AS ip_name,
      d.dnd_precedent_avg,
      d.dnd_precedent_count,
      m.mtg_precedent_avg,
      m.mtg_precedent_count,
      -- D&D 0.70 / MTG 0.30 weighted blend; if only one side has data,
      -- use that side directly (renormalization within the precedent
      -- source itself).
      ROUND(
        CASE
          WHEN d.dnd_precedent_avg IS NOT NULL
               AND m.mtg_precedent_avg IS NOT NULL THEN
            d.dnd_precedent_avg * 0.70 + m.mtg_precedent_avg * 0.30
          WHEN d.dnd_precedent_avg IS NOT NULL THEN d.dnd_precedent_avg
          WHEN m.mtg_precedent_avg IS NOT NULL THEN m.mtg_precedent_avg
          ELSE NULL
        END, 4
      ) AS precedent_score
    FROM dnd_precedent_per_ip d
    FULL OUTER JOIN mtg_precedent_per_ip m USING (ip_name)
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Source 3: BGG proxy (Stage 3)
  -- ────────────────────────────────────────────────────────────────────
  bgg_score AS (
    SELECT
      ip_name,
      bgg_proxy_score,
      product_archetype AS bgg_archetype
    FROM `dnd-trends-index.gold_data.ub_bgg_proxy`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Source 4: AO3 fanfic crossover proxy (Stage 4)
  -- ────────────────────────────────────────────────────────────────────
  ao3_score AS (
    SELECT
      ip_name,
      fanfic_proxy_score,
      ao3_work_count
    FROM `dnd-trends-index.gold_data.fanfic_crossover_proxy`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Source 5: Reddit D&D-community reception (Stage 5 Phase 1)
  -- ────────────────────────────────────────────────────────────────────
  reddit_score AS (
    SELECT
      ip_name,
      reddit_proxy_score,
      confirmed_mentions AS reddit_confirmed_mentions,
      reddit_status,
      reddit_signal_confidence
    FROM `dnd-trends-index.gold_data.reddit_reception_proxy`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Source 6: Homebrew creation intensity (combined v1 + v2 sub-streams)
  --
  -- Apr 29, 2026 v2 rewire: now reads from homebrew_combined_proxy which
  -- blends v1 (r/UnearthedArcana classified mentions) with v2 (GMBinder
  -- + Homebrewery confirmed-5e-homebrew artifacts after two-layer
  -- disambiguation). Surface column `homebrew_creation_score` retained
  -- for backwards-compat with the matrix output schema.
  -- ────────────────────────────────────────────────────────────────────
  homebrew_score AS (
    SELECT
      ip_name,
      homebrew_combined_score          AS homebrew_creation_score,
      ua_homebrew_mention_count        AS homebrew_mention_count,
      ua_sample_post_title             AS homebrew_sample_title,
      ua_homebrew_score                AS ua_homebrew_score,
      external_homebrew_score          AS external_homebrew_score,
      confirmed_5e_homebrew_count      AS confirmed_5e_homebrew_count,
      gmbinder_confirmed               AS gmbinder_confirmed,
      homebrewery_confirmed            AS homebrewery_confirmed,
      homebrew_sub_signals_present     AS homebrew_sub_signals_present
    FROM `dnd-trends-index.gold_data.homebrew_combined_proxy`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Source 7: Forum presence (EN World, GitP, RPG.net, Dragonsfoot)
  -- DM/whale demographic brand-purity signal. v1 is presence-only.
  -- ────────────────────────────────────────────────────────────────────
  forum_score AS (
    SELECT
      ip_name,
      forum_presence_score,
      total_results_combined AS forum_total_results,
      enworld_top_hits,
      giantitp_top_hits,
      rpgnet_top_hits,
      dragonsfoot_top_hits,
      top_thread_url AS forum_top_thread_url,
      top_thread_title AS forum_top_thread_title
    FROM `dnd-trends-index.gold_data.forum_presence_proxy`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Acquisition signal (Stage 5 Phase 2 — separate matrix dimension)
  -- ────────────────────────────────────────────────────────────────────
  acquisition_score AS (
    SELECT
      ip_name,
      reddit_acquisition_score,
      confirmed_mentions AS acquisition_confirmed_mentions,
      acquisition_status,
      acquisition_signal_confidence
    FROM `dnd-trends-index.gold_data.reddit_acquisition_proxy`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Existing license_fit_score from the original matrix view
  -- ────────────────────────────────────────────────────────────────────
  fit_score AS (
    SELECT
      ip_name,
      license_fit_score,
      rubric_composite,
      genre_fit, combat_translatability, party_dynamics_fit,
      setting_portability, fanbase_ttrpg_overlap
    FROM `dnd-trends-index.gold_data.universes_beyond_candidates`
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Join everything to the seed list (LEFT JOIN keeps all 142 IPs)
  -- ────────────────────────────────────────────────────────────────────
  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      f.license_fit_score,
      f.rubric_composite,
      g.gemini_baseline_score,
      pc.precedent_score,
      pc.dnd_precedent_count,
      pc.mtg_precedent_count,
      bgg.bgg_proxy_score,
      bgg.bgg_archetype,
      ao3.fanfic_proxy_score,
      ao3.ao3_work_count,
      rec.reddit_proxy_score,
      rec.reddit_confirmed_mentions,
      hb.homebrew_creation_score,
      hb.homebrew_mention_count,
      hb.homebrew_sample_title,
      hb.ua_homebrew_score,
      hb.external_homebrew_score,
      hb.confirmed_5e_homebrew_count,
      hb.gmbinder_confirmed,
      hb.homebrewery_confirmed,
      hb.homebrew_sub_signals_present,
      fm.forum_presence_score,
      fm.forum_total_results,
      fm.enworld_top_hits,
      fm.giantitp_top_hits,
      fm.rpgnet_top_hits,
      fm.dragonsfoot_top_hits,
      fm.forum_top_thread_url,
      fm.forum_top_thread_title,
      acq.reddit_acquisition_score,
      acq.acquisition_confirmed_mentions
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN gemini_score g USING (ip_name)
    LEFT JOIN precedent_combined pc USING (ip_name)
    LEFT JOIN bgg_score bgg USING (ip_name)
    LEFT JOIN ao3_score ao3 USING (ip_name)
    LEFT JOIN reddit_score rec USING (ip_name)
    LEFT JOIN homebrew_score hb USING (ip_name)
    LEFT JOIN forum_score fm USING (ip_name)
    LEFT JOIN acquisition_score acq USING (ip_name)
    LEFT JOIN fit_score f USING (ip_name)
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Composite scoring math.
  --
  -- For each IP, compute equal-weighted AND ChatGPT-weighted composites
  -- with per-IP renormalization. Renormalization works by summing the
  -- weights of the present sources and dividing the weighted sum by
  -- that total.
  -- ────────────────────────────────────────────────────────────────────
  composites AS (
    SELECT
      j.*,

      -- Count of MEASURED sources (excluding Gemini baseline).
      -- Stages 2-7 all count as measured.
      (
        IF(j.precedent_score          IS NOT NULL, 1, 0) +
        IF(j.bgg_proxy_score          IS NOT NULL, 1, 0) +
        IF(j.fanfic_proxy_score       IS NOT NULL, 1, 0) +
        IF(j.reddit_proxy_score       IS NOT NULL, 1, 0) +
        IF(j.homebrew_creation_score  IS NOT NULL, 1, 0) +
        IF(j.forum_presence_score     IS NOT NULL, 1, 0)
      ) AS measured_sources_count,

      -- Count of ALL sources (including Gemini)
      (
        IF(j.gemini_baseline_score    IS NOT NULL, 1, 0) +
        IF(j.precedent_score          IS NOT NULL, 1, 0) +
        IF(j.bgg_proxy_score          IS NOT NULL, 1, 0) +
        IF(j.fanfic_proxy_score       IS NOT NULL, 1, 0) +
        IF(j.reddit_proxy_score       IS NOT NULL, 1, 0) +
        IF(j.homebrew_creation_score  IS NOT NULL, 1, 0) +
        IF(j.forum_presence_score     IS NOT NULL, 1, 0)
      ) AS sources_present,

      -- ─── EQUAL-WEIGHTED COMPOSITE ───────────────────────────────────
      -- Each present source counts as 1; renormalize by total count.
      ROUND(
        SAFE_DIVIDE(
          COALESCE(j.gemini_baseline_score, 0)
          + COALESCE(j.precedent_score, 0)
          + COALESCE(j.bgg_proxy_score, 0)
          + COALESCE(j.fanfic_proxy_score, 0)
          + COALESCE(j.reddit_proxy_score, 0)
          + COALESCE(j.homebrew_creation_score, 0)
          + COALESCE(j.forum_presence_score, 0),
          NULLIF(
            IF(j.gemini_baseline_score    IS NOT NULL, 1, 0) +
            IF(j.precedent_score          IS NOT NULL, 1, 0) +
            IF(j.bgg_proxy_score          IS NOT NULL, 1, 0) +
            IF(j.fanfic_proxy_score       IS NOT NULL, 1, 0) +
            IF(j.reddit_proxy_score       IS NOT NULL, 1, 0) +
            IF(j.homebrew_creation_score  IS NOT NULL, 1, 0) +
            IF(j.forum_presence_score     IS NOT NULL, 1, 0),
            0
          )
        ),
        4
      ) AS composite_score_equal_raw,

      -- ─── WEIGHTED COMPOSITE (ChatGPT revised weights + Stages 6+7) ──
      -- Precedents 0.30 / BGG 0.20 / AO3 0.20 / Reddit 0.15 /
      -- Gemini 0.15 / Homebrew 0.10 / Forum 0.10
      -- Per-IP renormalization handles the un-summing-to-1.0 — only
      -- present-source weights enter the denominator.
      ROUND(
        SAFE_DIVIDE(
          COALESCE(j.precedent_score, 0)         * 0.30 +
          COALESCE(j.bgg_proxy_score, 0)         * 0.20 +
          COALESCE(j.fanfic_proxy_score, 0)      * 0.20 +
          COALESCE(j.reddit_proxy_score, 0)      * 0.15 +
          COALESCE(j.gemini_baseline_score, 0)   * 0.15 +
          COALESCE(j.homebrew_creation_score, 0) * 0.10 +
          COALESCE(j.forum_presence_score, 0)    * 0.10,
          NULLIF(
            IF(j.precedent_score          IS NOT NULL, 0.30, 0) +
            IF(j.bgg_proxy_score          IS NOT NULL, 0.20, 0) +
            IF(j.fanfic_proxy_score       IS NOT NULL, 0.20, 0) +
            IF(j.reddit_proxy_score       IS NOT NULL, 0.15, 0) +
            IF(j.gemini_baseline_score    IS NOT NULL, 0.15, 0) +
            IF(j.homebrew_creation_score  IS NOT NULL, 0.10, 0) +
            IF(j.forum_presence_score     IS NOT NULL, 0.10, 0),
            0
          )
        ),
        4
      ) AS composite_score_weighted_raw,

      -- ─── CONSILIENCE METRICS ────────────────────────────────────────
      (
        IF(COALESCE(j.gemini_baseline_score, 0)   >= 0.7, 1, 0) +
        IF(COALESCE(j.precedent_score, 0)         >= 0.7, 1, 0) +
        IF(COALESCE(j.bgg_proxy_score, 0)         >= 0.7, 1, 0) +
        IF(COALESCE(j.fanfic_proxy_score, 0)      >= 0.7, 1, 0) +
        IF(COALESCE(j.reddit_proxy_score, 0)      >= 0.7, 1, 0) +
        IF(COALESCE(j.homebrew_creation_score, 0) >= 0.7, 1, 0) +
        IF(COALESCE(j.forum_presence_score, 0)    >= 0.7, 1, 0)
      ) AS sources_positive,

      (
        IF(j.gemini_baseline_score    IS NOT NULL AND j.gemini_baseline_score    <= 0.3, 1, 0) +
        IF(j.precedent_score          IS NOT NULL AND j.precedent_score          <= 0.3, 1, 0) +
        IF(j.bgg_proxy_score          IS NOT NULL AND j.bgg_proxy_score          <= 0.3, 1, 0) +
        IF(j.fanfic_proxy_score       IS NOT NULL AND j.fanfic_proxy_score       <= 0.3, 1, 0) +
        IF(j.reddit_proxy_score       IS NOT NULL AND j.reddit_proxy_score       <= 0.3, 1, 0) +
        IF(j.homebrew_creation_score  IS NOT NULL AND j.homebrew_creation_score  <= 0.3, 1, 0) +
        IF(j.forum_presence_score     IS NOT NULL AND j.forum_presence_score     <= 0.3, 1, 0)
      ) AS sources_negative,

      -- Measured-source variance (for highly_corroborated flag).
      -- Includes Stages 2-7 (all measured).
      (
        SELECT MAX(s) - MIN(s)
        FROM UNNEST([
          j.precedent_score, j.bgg_proxy_score,
          j.fanfic_proxy_score, j.reddit_proxy_score,
          j.homebrew_creation_score, j.forum_presence_score
        ]) AS s
        WHERE s IS NOT NULL
      ) AS measured_score_range
    FROM joined j
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Apply abstention rules
  -- ────────────────────────────────────────────────────────────────────
  with_abstention AS (
    SELECT
      c.*,
      CASE
        WHEN c.measured_sources_count >= 2 THEN c.composite_score_equal_raw
        ELSE NULL
      END AS composite_score_equal,
      CASE
        WHEN c.measured_sources_count >= 2 THEN c.composite_score_weighted_raw
        ELSE NULL
      END AS composite_score_weighted,
      CASE
        WHEN c.measured_sources_count >= 2 THEN 'sufficient'
        WHEN c.measured_sources_count = 1 THEN 'thin_evidence'
        WHEN c.gemini_baseline_score IS NOT NULL THEN 'modeled_only_low_confidence'
        ELSE 'no_data'
      END AS composite_status
    FROM composites c
  )

SELECT
  w.ip_name,
  w.medium,
  w.tier,

  -- ─── EXISTING FIT DIMENSION (carried through for the matrix) ──────────
  w.license_fit_score,
  w.rubric_composite,

  -- ─── COMMUNITY RECEPTION COMPOSITE — DUAL VIEW ────────────────────────
  w.composite_score_equal AS community_reception_score_equal,
  w.composite_score_weighted AS community_reception_score_weighted,
  CASE
    WHEN w.composite_score_equal IS NOT NULL
         AND w.composite_score_weighted IS NOT NULL THEN
      ROUND(ABS(w.composite_score_equal - w.composite_score_weighted), 4)
    ELSE NULL
  END AS score_divergence,
  w.composite_status,

  -- ─── 7 RECEPTION SUB-SCORES (always shown for the data trail) ─────────
  w.gemini_baseline_score,
  w.precedent_score,
  w.bgg_proxy_score,
  w.fanfic_proxy_score,
  w.reddit_proxy_score,
  w.homebrew_creation_score,
  w.forum_presence_score,

  -- ─── ACQUISITION DIMENSION (separate axis on the matrix) ──────────────
  w.reddit_acquisition_score,
  w.acquisition_confirmed_mentions,

  -- ─── CONSILIENCE METRICS ──────────────────────────────────────────────
  w.measured_sources_count,
  w.sources_present,
  w.sources_positive,
  w.sources_negative,
  CASE
    WHEN w.sources_present > 0 THEN
      ROUND(
        GREATEST(w.sources_positive, w.sources_negative) / w.sources_present,
        4
      )
    ELSE NULL
  END AS consilience_score,

  -- Highly Corroborated badge: 3+ measured sources within 0.2 variance
  CASE
    WHEN w.measured_sources_count >= 3
         AND w.measured_score_range IS NOT NULL
         AND w.measured_score_range <= 0.2 THEN TRUE
    ELSE FALSE
  END AS highly_corroborated,

  -- ─── BCG QUADRANT (2x2 reception × acquisition) ──────────────────────
  -- Uses equal-weighted composite for the demo narrative. Thresholds
  -- HIGH=0.60 / LOW=0.40 chosen empirically against our actual data
  -- distribution. Mid-zone (0.40 to 0.60) gets NULL — no confident
  -- classification.
  CASE
    WHEN w.composite_score_equal IS NULL
         OR w.reddit_acquisition_score IS NULL THEN NULL
    WHEN w.composite_score_equal >= 0.60 AND w.reddit_acquisition_score >= 0.60
      THEN 'gold_mine'
    WHEN w.composite_score_equal >= 0.60 AND w.reddit_acquisition_score < 0.40
      THEN 'fan_service'
    WHEN w.composite_score_equal < 0.40 AND w.reddit_acquisition_score >= 0.60
      THEN 'trojan_horse'
    WHEN w.composite_score_equal < 0.40 AND w.reddit_acquisition_score < 0.40
      THEN 'brand_hazard'
    ELSE NULL  -- mid-zone, no confident classification
  END AS quadrant,

  -- ─── ASYMMETRY FLAGS (pill-badge candidates for the UI) ──────────────
  -- High-fit / low-reception risk — use the original 5-dim fit score.
  -- 0.65 fit threshold catches Spy x Family (0.66 fit, 0.20 composite).
  CASE
    WHEN w.license_fit_score >= 0.65
         AND w.composite_score_equal IS NOT NULL
         AND w.composite_score_equal <= 0.4 THEN TRUE
    ELSE FALSE
  END AS is_high_backlash_risk,

  -- Reception-vs-acquisition asymmetry (Stranger Things pattern):
  -- D&D Reddit users want the IP, but IP-side users don't want D&D back.
  -- Uses REDDIT-ONLY signals (not composite) because the asymmetry is
  -- between same-source Reddit data on opposite sides of the funnel.
  -- Composite reception is averaged with BGG/AO3/etc. which can pull
  -- the score away from the pure Reddit-side "D&D players want it"
  -- signal we want to detect.
  CASE
    WHEN w.reddit_proxy_score IS NOT NULL
         AND w.reddit_proxy_score >= 0.7
         AND w.reddit_acquisition_score IS NOT NULL
         AND w.reddit_acquisition_score <= 0.6 THEN TRUE
    ELSE FALSE
  END AS is_engagement_only,

  -- Mirror of engagement_only: IP fans want D&D, D&D players don't
  -- particularly want the IP. Acquisition opportunity for new
  -- customers without satisfying existing-customer demand.
  CASE
    WHEN w.reddit_acquisition_score IS NOT NULL
         AND w.reddit_acquisition_score >= 0.7
         AND w.reddit_proxy_score IS NOT NULL
         AND w.reddit_proxy_score <= 0.5 THEN TRUE
    ELSE FALSE
  END AS is_new_player_magnet,

  -- Phase-2 unique unlock (Dungeon Crawler Carl pattern):
  -- acquisition has signal but reception score wasn't reachable
  CASE
    WHEN w.reddit_acquisition_score IS NOT NULL
         AND w.composite_score_equal IS NULL THEN TRUE
    ELSE FALSE
  END AS is_phase2_unlock,

  -- ─── SUB-SOURCE CONTEXT (data trail for the UI) ───────────────────────
  w.dnd_precedent_count,
  w.mtg_precedent_count,
  w.bgg_archetype,
  w.ao3_work_count,
  w.reddit_confirmed_mentions,
  w.homebrew_mention_count,
  w.homebrew_sample_title,
  -- v2 homebrew sub-trail (Apr 29 — combined UA Reddit + external GMBinder/Homebrewery)
  w.ua_homebrew_score,
  w.external_homebrew_score,
  w.confirmed_5e_homebrew_count,
  w.gmbinder_confirmed,
  w.homebrewery_confirmed,
  w.homebrew_sub_signals_present,
  w.forum_total_results,
  w.enworld_top_hits,
  w.giantitp_top_hits,
  w.rpgnet_top_hits,
  w.dragonsfoot_top_hits,
  w.forum_top_thread_url,
  w.forum_top_thread_title,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN w.composite_status = 'no_data' THEN
      'No sub-scores available — IP not yet measured.'
    WHEN w.composite_status = 'modeled_only_low_confidence' THEN
      CONCAT(
        'Only Gemini baseline (', CAST(w.gemini_baseline_score AS STRING),
        ') — no measured sources. Treat as weak prior; do not act on this alone.'
      )
    WHEN w.composite_status = 'thin_evidence' THEN
      CONCAT(
        'Only 1 measured source — too thin for confident composite. ',
        'Equal: ', CAST(w.composite_score_equal_raw AS STRING),
        ' / Weighted: ', CAST(w.composite_score_weighted_raw AS STRING),
        ' (shown for reference, NULL in score columns).'
      )
    ELSE
      CONCAT(
        'Composite from ', CAST(w.measured_sources_count AS STRING),
        ' measured + ',
        CAST(IF(w.gemini_baseline_score IS NOT NULL, 1, 0) AS STRING),
        ' baseline source(s). Equal=',
        CAST(w.composite_score_equal AS STRING),
        ', Weighted=', CAST(w.composite_score_weighted AS STRING),
        ', Divergence=',
        CAST(ROUND(ABS(w.composite_score_equal - w.composite_score_weighted), 3) AS STRING),
        '.'
      )
  END AS composite_reasoning,

  -- ─── STANDARDIZED OUTPUT CONTRACT ─────────────────────────────────────
  'community_reception' AS signal_type,
  'ub_matrix_composite' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM with_abstention w
ORDER BY
  -- Order by quadrant (gold_mine first, brand_hazard last), then
  -- by composite_score_equal DESC, then by sources_present DESC.
  CASE w.composite_status
    WHEN 'sufficient' THEN 1
    WHEN 'thin_evidence' THEN 2
    WHEN 'modeled_only_low_confidence' THEN 3
    ELSE 4
  END,
  CASE
    WHEN w.composite_score_equal IS NULL THEN -1
    ELSE w.composite_score_equal
  END DESC NULLS LAST;
