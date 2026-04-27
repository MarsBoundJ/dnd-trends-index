-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.ub_bgg_proxy — Stage 3 of community_reception composite
-- ═══════════════════════════════════════════════════════════════════════
--
-- For each Universes Beyond candidate IP that has a licensed board game
-- adaptation on BoardGameGeek, scores how the IP's existing tabletop
-- adaptation was received — used as a proxy signal for community_reception.
--
-- The premise: tabletop gamers (BGG voters) are a closer audience proxy
-- to D&D players than general consumers. If the Dark Souls Board Game
-- got tepid quality scores from BGG voters, that skepticism likely
-- transfers to a hypothetical Dark Souls D&D set. If LotR's classic
-- Knizia game has 28k+ owners with stable ratings, the IP carries
-- proven tabletop reception.
--
-- ─── DATA FLOW ──────────────────────────────────────────────────────────
--
-- This view sits on top of an existing pipeline:
--
--   scripts/seed_ub_bgg_proxy_ids.py          (curated map: 41 IP → BGG IDs)
--          ↓
--   scripts/load_ub_bgg_proxy_ids.py
--          ↓
--   dnd_trends_categorized.bgg_id_map         (concept_name = ip_name)
--          ↓ (Mon/Wed/Fri 3am UTC scheduled harvest)
--   harvesters/bgg_harvester.py
--          ↓
--   dnd_trends_raw.bgg_product_stats          (daily snapshots)
--          ↓
--   gold_data.analytics_bgg                   (latest + archetype computation)
--          ↓
--   gold_data.ub_bgg_proxy                    (THIS VIEW)
--          ↓ (consumed by future composite community_reception view)
--
-- ─── SCORE FORMULA ──────────────────────────────────────────────────────
--
-- bgg_proxy_score lives on the same [0.0, 1.0] scale as the other
-- community_reception sub-scores (Gemini baseline, MTG UB precedent
-- match, AO3 crossover, Reddit sentiment). High = better received.
--
-- Mapping product_archetype + best_quality to a bgg_proxy_score band:
--
--   mainstream_hit   → 0.70-0.95  ('hit' games like LotR, Dune)
--   cult_classic     → 0.65-0.85  (loved-by-few like Stormlight CtA)
--   established      → 0.40-0.70  (mid-tier games)
--   buyers_remorse   → 0.10-0.35  (CASH-GRAB DETECTOR — high owned, low rating)
--   sleeper          → NULL       (insufficient signal — drops out via renormalization)
--
-- Within each band, BGG's best_quality (rating 0-10) drives the position.
-- Linear within-band: lowest quality in the band = floor, highest = cap.
--
-- ─── ARCHETYPE → COMMUNITY_RECEPTION INTERPRETATION ────────────────────
--
-- mainstream_hit (≥50 owners + quality ≥6.0):
--   "The licensed board game is broadly owned and decently rated. Tabletop
--    gamers welcomed this IP's tabletop translation. Strong signal that
--    a D&D crossover would also land well."
--
-- cult_classic (<20 owners + quality ≥8.0):
--   "Few people own the board game but those who do love it. Suggests
--    the IP has a small-but-devoted tabletop-curious audience — a D&D
--    crossover would land well within that audience even if it didn't
--    sell mainstream."
--
-- buyers_remorse (≥30 owners + quality <6.0):
--   "People bought the licensed board game but rated it poorly — the
--    'cash grab' pattern. Strong negative signal: tabletop gamers feel
--    burned by this IP's tabletop translation, would view a D&D
--    crossover with skepticism. THIS IS THE STRATEGIC-VALUE SIGNAL."
--
-- established (default, anything else with rating data):
--   "Mid-tier reception. Some signal but not strongly positive or
--    negative — IP carries no obvious tabletop trust deficit but no
--    obvious tabletop enthusiasm either."
--
-- sleeper (<10 owners, no clear quality signal):
--   "Too few people own the licensed adaptation to extract reception
--    signal. The mapping exists but the data is too thin to trust.
--    NULL bgg_proxy_score — the eventual composite renormalizes other
--    signals to compensate."
--
-- ─── CONFIDENCE DERIVATION ──────────────────────────────────────────────
--
-- bgg_signal_confidence is derived from data, not from the curator's
-- HIGH/MEDIUM/LOW tag in seed_ub_bgg_proxy_ids.py — letting the data
-- speak is more honest than carrying curation bias forward.
--
--   HIGH:    owned ≥1000 AND quality_data_present
--   MEDIUM:  owned ≥100 AND quality_data_present
--   LOW:     owned <100 OR quality unrated
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.ub_bgg_proxy` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Join UB candidates to their BGG-adapted board game stats.  LEFT
  -- JOIN keeps all 142 UB IPs; unmapped IPs get NULL across BGG fields.
  -- ────────────────────────────────────────────────────────────────────
  ub_with_bgg AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      s.disambiguation,

      -- BGG join key + linked-game identity
      m.bgg_id,
      a.concept_name AS bgg_game_name,
      a.product_archetype,
      a.max_owned,
      a.best_quality,
      a.commitment_index,
      a.platform_presence,
      a.bgg_date AS latest_bgg_snapshot_date

    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN `dnd-trends-index.dnd_trends_categorized.bgg_id_map` m
      ON m.concept_name = s.ip_name AND m.platform = 'bgg'
    LEFT JOIN `dnd-trends-index.gold_data.analytics_bgg` a
      ON a.concept_name = s.ip_name
  )

SELECT
  u.ip_name,
  u.medium,
  u.tier,

  -- Identity / linked-game data trail
  u.bgg_id,
  u.bgg_game_name,
  u.product_archetype,
  u.max_owned,
  ROUND(u.best_quality, 2) AS bgg_quality_score,
  u.commitment_index,
  u.platform_presence,
  u.latest_bgg_snapshot_date,

  -- ─── THE PROXY SCORE ──────────────────────────────────────────────────
  -- Maps archetype + quality to a [0, 1] reception band aligned with
  -- the rest of the community_reception sub-scores.  NULL when archetype
  -- is sleeper (too thin a signal) or when the IP has no BGG mapping
  -- at all (most IPs without a licensed adaptation).
  CASE
    -- Mainstream hit: 0.70-0.95 band, scaled by quality (6.0-10.0)
    WHEN u.product_archetype = 'mainstream_hit' THEN
      ROUND(0.70 + LEAST(GREATEST(u.best_quality - 6.0, 0), 4.0) / 4.0 * 0.25, 3)

    -- Cult classic: 0.65-0.85 band, scaled by quality (8.0-10.0)
    WHEN u.product_archetype = 'cult_classic' THEN
      ROUND(0.65 + LEAST(GREATEST(u.best_quality - 8.0, 0), 2.0) / 2.0 * 0.20, 3)

    -- Established: 0.40-0.70 band, scaled by quality (5.0-10.0)
    WHEN u.product_archetype = 'established' THEN
      ROUND(0.40 + LEAST(GREATEST(u.best_quality - 5.0, 0), 5.0) / 5.0 * 0.30, 3)

    -- Buyers remorse (CASH GRAB): 0.10-0.35 band, INVERSELY scaled
    -- by quality.  Worse quality (closer to 0) = lower score (closer
    -- to 0.10).  Better quality within the buyers_remorse threshold
    -- (closer to 6.0) = higher score (closer to 0.35).
    WHEN u.product_archetype = 'buyers_remorse' THEN
      ROUND(0.10 + LEAST(GREATEST(u.best_quality - 4.0, 0), 2.0) / 2.0 * 0.25, 3)

    -- Sleeper or missing: NULL (signal too thin / no signal)
    ELSE NULL
  END AS bgg_proxy_score,

  -- ─── CONFIDENCE (derived from data, not curator opinion) ──────────────
  CASE
    WHEN u.product_archetype IS NULL THEN 'NONE'
    WHEN u.product_archetype = 'sleeper' THEN 'LOW'
    WHEN u.max_owned >= 1000 AND u.best_quality > 0 THEN 'HIGH'
    WHEN u.max_owned >= 100 AND u.best_quality > 0 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS bgg_signal_confidence,

  -- ─── HUMAN-READABLE REASONING for the data trail ──────────────────────
  CASE
    WHEN u.product_archetype IS NULL THEN
      'No licensed BGG board game adaptation in our curated map.'
    WHEN u.product_archetype = 'mainstream_hit' THEN
      CONCAT(
        'BGG mainstream hit: "', u.bgg_game_name, '" — ',
        CAST(u.max_owned AS STRING), ' owners, quality ',
        CAST(ROUND(u.best_quality, 1) AS STRING),
        '. Tabletop gamers welcomed this board-game translation.'
      )
    WHEN u.product_archetype = 'cult_classic' THEN
      CONCAT(
        'BGG cult classic: "', u.bgg_game_name, '" — small audience but ',
        'beloved (quality ', CAST(ROUND(u.best_quality, 1) AS STRING),
        '). Devoted tabletop niche, would translate well to D&D.'
      )
    WHEN u.product_archetype = 'buyers_remorse' THEN
      CONCAT(
        'BGG cash-grab pattern: "', u.bgg_game_name, '" — ',
        CAST(u.max_owned AS STRING), ' owners but quality only ',
        CAST(ROUND(u.best_quality, 1) AS STRING),
        '. Tabletop gamers felt burned — D&D crossover faces trust deficit.'
      )
    WHEN u.product_archetype = 'sleeper' THEN
      CONCAT(
        'BGG sleeper: "', u.bgg_game_name, '" — only ',
        CAST(u.max_owned AS STRING),
        ' owners, insufficient signal to score reception.'
      )
    ELSE
      CONCAT(
        'BGG established: "', u.bgg_game_name, '" — quality ',
        CAST(ROUND(u.best_quality, 1) AS STRING),
        ', mid-tier reception.'
      )
  END AS bgg_proxy_reasoning,

  -- Standardized output contract (matches other gold_data views)
  'community_reception' AS signal_type,
  'bgg_licensed_game_proxy' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM ub_with_bgg u
ORDER BY
  -- Best signals first when scanning interactively
  CASE u.product_archetype
    WHEN 'buyers_remorse' THEN 1  -- The cash-grab cases lead — strategic value
    WHEN 'mainstream_hit' THEN 2
    WHEN 'cult_classic'   THEN 3
    WHEN 'established'    THEN 4
    WHEN 'sleeper'        THEN 5
    ELSE 6
  END,
  u.max_owned DESC;
