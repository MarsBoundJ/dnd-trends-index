-- Hype vs Play Divergence: All Talk vs Hidden Gems
-- Dataset: gold_data.composite_hype_vs_play
-- Reads from: gold_data.composite_concept_index (materialized daily)
--
-- Compares buzz (curiosity + community) against actual play commitment
-- (ownership: Roll20, BGG, Steam). Concepts where people talk a lot but
-- don't actually play are "all talk." Concepts played heavily but rarely
-- discussed are "hidden gems" that WotC could amplify.
--
-- Key insight: This tells WotC where marketing should push vs where
-- product investment is wasted on hype that doesn't convert to play.
--
-- Usage:
--   -- Hidden gems WotC should market:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_hype_vs_play`
--   WHERE divergence_class = 'hidden_gem' ORDER BY play_score DESC
--
--   -- All talk (hype doesn't convert):
--   SELECT * FROM `dnd-trends-index.gold_data.composite_hype_vs_play`
--   WHERE divergence_class = 'all_talk' ORDER BY hype_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_hype_vs_play` AS

WITH base AS (
  SELECT
    concept_name,
    category,
    ruleset,
    canonical_parent,

    -- Hype = curiosity + community (talk signals)
    curiosity_score,
    community_score,
    demand_score AS hype_score,  -- demand_score = avg(curiosity, community)

    -- Play = ownership (actual adoption)
    ownership_score,
    ownership_score AS play_score,

    -- Supporting context
    creator_score,
    commerce_score,
    talk_to_play_ratio,
    trend_level,
    trend_momentum,
    streams_present,

    -- Momentum for both sides
    curiosity_momentum,
    community_momentum,
    ownership_momentum

  FROM `dnd-trends-index.gold_data.composite_concept_index`
  -- Require at least hype OR play data
  WHERE (curiosity_score IS NOT NULL OR community_score IS NOT NULL
         OR ownership_score IS NOT NULL)
    AND streams_present >= 2
),

classified AS (
  SELECT
    *,

    -- Hype-play gap: positive = more hype than play
    ROUND(COALESCE(hype_score, 0) - COALESCE(play_score, 0), 4) AS hype_play_gap,

    -- Divergence classification
    CASE
      -- HIDDEN GEM: high play, low hype — WotC should market these
      WHEN COALESCE(play_score, 0) >= 0.5
       AND COALESCE(hype_score, 0) < 0.4
        THEN 'hidden_gem'

      -- ALL TALK: high hype, low play — don't over-invest
      WHEN COALESCE(hype_score, 0) >= 0.5
       AND COALESCE(play_score, 0) < 0.4
        THEN 'all_talk'

      -- VALIDATED HIT: high hype AND high play — working as intended
      WHEN COALESCE(hype_score, 0) >= 0.5
       AND COALESCE(play_score, 0) >= 0.5
        THEN 'validated_hit'

      -- QUIET WORKHORSE: moderate play, low hype — reliable but unexciting
      WHEN COALESCE(play_score, 0) >= 0.3
       AND COALESCE(hype_score, 0) < 0.3
        THEN 'quiet_workhorse'

      -- EMERGING BUZZ: rising hype, play data not yet visible
      WHEN COALESCE(hype_score, 0) >= 0.3
       AND play_score IS NULL
        THEN 'emerging_buzz'

      -- NICHE: present but low on both
      ELSE 'niche'
    END AS divergence_class,

    -- Conversion potential: does hype have momentum to convert to play?
    CASE
      WHEN COALESCE(curiosity_momentum, 0.5) > 0.6
       AND COALESCE(ownership_momentum, 0.5) > 0.6
        THEN 'converting'       -- hype is turning into play
      WHEN COALESCE(curiosity_momentum, 0.5) > 0.6
       AND COALESCE(ownership_momentum, 0.5) <= 0.5
        THEN 'hype_building'    -- hype rising but play hasn't followed yet
      WHEN COALESCE(curiosity_momentum, 0.5) <= 0.4
       AND COALESCE(ownership_momentum, 0.5) > 0.6
        THEN 'organic_adoption' -- play growing without hype
      WHEN COALESCE(curiosity_momentum, 0.5) < 0.4
       AND COALESCE(ownership_momentum, 0.5) < 0.4
        THEN 'declining'        -- both fading
      ELSE 'stable'
    END AS conversion_signal,

    -- Creator investment: are creators bridging hype to play?
    CASE
      WHEN creator_score IS NOT NULL AND creator_score >= 0.6 THEN 'high_creator_investment'
      WHEN creator_score IS NOT NULL AND creator_score >= 0.3 THEN 'moderate_creator_investment'
      ELSE 'low_creator_investment'
    END AS creator_bridge

  FROM base
)

SELECT
  concept_name,
  category,
  ruleset,
  canonical_parent,

  -- Classification
  divergence_class,
  conversion_signal,
  creator_bridge,

  -- Core scores
  ROUND(hype_score, 3)  AS hype_score,
  ROUND(play_score, 3)  AS play_score,
  ROUND(hype_play_gap, 3) AS hype_play_gap,
  ROUND(talk_to_play_ratio, 2) AS talk_to_play_ratio,

  -- Bucket detail
  curiosity_score,
  community_score,
  ownership_score,
  creator_score,
  commerce_score,

  -- Momentum
  curiosity_momentum,
  community_momentum,
  ownership_momentum,

  -- Confidence
  streams_present,
  CASE
    WHEN streams_present >= 5
     AND play_score IS NOT NULL
     AND hype_score IS NOT NULL THEN 'HIGH'
    WHEN streams_present >= 3 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS classification_confidence,

  -- Overall
  trend_level,
  trend_momentum,

  -- Rank: within divergence class
  ROW_NUMBER() OVER (
    PARTITION BY CASE
      WHEN COALESCE(play_score, 0) >= 0.5 AND COALESCE(hype_score, 0) < 0.4 THEN 'hidden_gem'
      WHEN COALESCE(hype_score, 0) >= 0.5 AND COALESCE(play_score, 0) < 0.4 THEN 'all_talk'
      WHEN COALESCE(hype_score, 0) >= 0.5 AND COALESCE(play_score, 0) >= 0.5 THEN 'validated_hit'
      WHEN COALESCE(play_score, 0) >= 0.3 AND COALESCE(hype_score, 0) < 0.3 THEN 'quiet_workhorse'
      WHEN COALESCE(hype_score, 0) >= 0.3 AND play_score IS NULL THEN 'emerging_buzz'
      ELSE 'niche'
    END
    ORDER BY ABS(COALESCE(hype_score, 0) - COALESCE(play_score, 0)) DESC
  ) AS rank_in_class,

  -- Rank: within category
  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY ABS(COALESCE(hype_score, 0) - COALESCE(play_score, 0)) DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM classified;
