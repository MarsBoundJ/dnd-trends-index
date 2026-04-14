-- Mechanics Friction Heatmap: What Rules Are Confusing Your Players?
-- Dataset: gold_data.composite_mechanics_friction
-- Reads from: analytics_fandom + analytics_reddit + concept_library
--
-- Inspired by Gemini's "Rules & Mechanics Friction" idea.
-- Unique insight: No other data provider tells WotC which mechanics are broken.
--
-- Logic:
--   High Fandom Wiki lookup volume = people are confused and looking things up
--   Negative Reddit sentiment for the same concept = people are complaining
--   Combined = quantified "painpoint" score for game designers
--
-- Filters to mechanics-adjacent categories only:
--   Combat Mechanics, core_mechanics, Mechanic, Core Mechanic, Combat Tactics,
--   Ability Scores, Condition, Damage Type, Movement, Action, Spell, Class, Feat
--
-- Usage:
--   -- Top friction painpoints:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_mechanics_friction`
--   WHERE friction_class = 'painpoint' ORDER BY friction_score DESC
--
--   -- Controversial mechanics (high engagement, split opinions):
--   SELECT * FROM `dnd-trends-index.gold_data.composite_mechanics_friction`
--   WHERE friction_class = 'controversial' ORDER BY reddit_volume DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_mechanics_friction` AS

WITH -- Step 1: Filter concept library to mechanics-related categories
mechanic_concepts AS (
  SELECT
    concept_name,
    category,
    ruleset,
    canonical_parent
  FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
  WHERE is_active = TRUE
    AND category IN (
      'Combat Mechanics', 'core_mechanics', 'Mechanic', 'Core Mechanic',
      'Combat Tactics', 'Combat', 'Ability Scores', 'Condition',
      'Damage Type', 'Movement', 'Action', 'Game Mechanic',
      -- Include spells, classes, feats — players look these up when confused
      'Spell', 'Class', 'Subclass', 'Feat', 'Invocation',
      'Race', 'Background'
    )
),

-- Step 2: Fandom wiki signals for mechanic concepts (confusion indicator)
-- High views + high edits on a rule page = people are looking it up = confused
fandom_signals AS (
  SELECT
    f.concept_name,
    f.hype_score,
    f.edits_30d,
    f.views_30d,
    f.edits_7d,
    f.views_7d,
    f.edit_velocity_7d,
    f.lore_depth_class,
    -- Confusion proxy: high views relative to edits means people reading, not editing
    -- (they need to look up the rule but can't figure it out enough to fix the wiki)
    f.view_to_edit_ratio,
    -- Normalize within fandom: how does this mechanic compare to all fandom articles?
    PERCENT_RANK() OVER (ORDER BY f.views_30d ASC) AS fandom_view_percentile,
    PERCENT_RANK() OVER (ORDER BY f.edits_30d ASC) AS fandom_edit_percentile,
    PERCENT_RANK() OVER (ORDER BY f.hype_score ASC) AS fandom_hype_percentile
  FROM `dnd-trends-index.gold_data.analytics_fandom` f
  JOIN mechanic_concepts mc ON LOWER(f.concept_name) = LOWER(mc.concept_name)
),

-- Step 3: Reddit signals for mechanic concepts (complaint indicator)
reddit_signals AS (
  SELECT
    r.concept_name,
    r.mention_count_30d,
    r.sentiment_avg,
    r.sentiment_7d,
    r.heat_profile,
    r.subreddit_spread,
    r.viral_events_30d,
    r.momentum_7d AS reddit_momentum,
    -- Normalize within reddit
    PERCENT_RANK() OVER (ORDER BY r.mention_count_30d ASC) AS reddit_volume_percentile,
    -- Invert sentiment: low sentiment = high friction signal
    -- sentiment_avg is the weighted_score which is typically positive for upvoted content
    -- Lower values = more negative/controversial
    PERCENT_RANK() OVER (ORDER BY r.sentiment_avg ASC) AS reddit_negativity_percentile
  FROM `dnd-trends-index.gold_data.analytics_reddit` r
  JOIN mechanic_concepts mc ON LOWER(r.concept_name) = LOWER(mc.concept_name)
),

-- Step 4: Google Trends signals (are people searching for help with this rule?)
trends_signals AS (
  SELECT
    g.concept_name,
    g.current_interest,
    g.momentum_14d AS trends_momentum,
    g.lifecycle_state,
    PERCENT_RANK() OVER (ORDER BY g.current_interest ASC) AS trends_percentile
  FROM `dnd-trends-index.gold_data.analytics_google_trends` g
  JOIN mechanic_concepts mc ON LOWER(g.concept_name) = LOWER(mc.concept_name)
),

-- Step 5: Combine signals
combined AS (
  SELECT
    mc.concept_name,
    mc.category,
    mc.ruleset,
    mc.canonical_parent,

    -- Fandom (confusion signal)
    f.views_30d AS fandom_views,
    f.edits_30d AS fandom_edits,
    f.edits_7d AS fandom_edits_7d,
    f.view_to_edit_ratio,
    f.fandom_view_percentile,
    f.fandom_edit_percentile,
    f.fandom_hype_percentile,
    f.lore_depth_class,
    f.edit_velocity_7d,

    -- Reddit (complaint signal)
    r.mention_count_30d AS reddit_volume,
    r.sentiment_avg AS reddit_sentiment,
    r.sentiment_7d AS reddit_sentiment_7d,
    r.heat_profile AS reddit_heat_profile,
    r.subreddit_spread,
    r.viral_events_30d,
    r.reddit_volume_percentile,
    r.reddit_negativity_percentile,

    -- Google Trends (search-for-help signal)
    t.current_interest AS trends_interest,
    t.trends_percentile,
    t.lifecycle_state AS trends_lifecycle,

    -- =====================================================
    -- FRICTION SCORE: combines confusion + complaint + search
    -- =====================================================
    -- Fandom lookup intensity (40% weight) — are people confused?
    -- Reddit negativity (35% weight) — are people complaining?
    -- Google search intensity (25% weight) — are people searching for help?
    ROUND(
      COALESCE(f.fandom_view_percentile, 0) * 0.40 +
      COALESCE(r.reddit_negativity_percentile, 0) * 0.35 +
      COALESCE(t.trends_percentile, 0) * 0.25
    , 4) AS friction_score,

    -- Data source flags
    CASE WHEN f.concept_name IS NOT NULL THEN 1 ELSE 0 END AS has_fandom,
    CASE WHEN r.concept_name IS NOT NULL THEN 1 ELSE 0 END AS has_reddit,
    CASE WHEN t.concept_name IS NOT NULL THEN 1 ELSE 0 END AS has_trends

  FROM mechanic_concepts mc
  LEFT JOIN fandom_signals f ON LOWER(mc.concept_name) = LOWER(f.concept_name)
  LEFT JOIN reddit_signals r ON LOWER(mc.concept_name) = LOWER(r.concept_name)
  LEFT JOIN trends_signals t ON LOWER(mc.concept_name) = LOWER(t.concept_name)
  -- Must have data from at least one source
  WHERE f.concept_name IS NOT NULL
     OR r.concept_name IS NOT NULL
     OR t.concept_name IS NOT NULL
)

SELECT
  concept_name,
  category,
  ruleset,
  canonical_parent,

  -- Friction score and classification
  friction_score,
  CASE
    -- PAINPOINT: high friction across multiple signals
    WHEN friction_score >= 0.7
     AND (has_fandom + has_reddit + has_trends) >= 2
      THEN 'painpoint'

    -- CONTROVERSIAL: high Reddit volume + split/negative sentiment
    WHEN reddit_volume_percentile >= 0.7
     AND reddit_negativity_percentile >= 0.6
      THEN 'controversial'

    -- CONFUSING: high Fandom lookups but low Reddit discussion
    -- (people silently struggling, not complaining publicly)
    WHEN fandom_view_percentile >= 0.7
     AND COALESCE(reddit_volume_percentile, 0) < 0.4
      THEN 'confusing'

    -- HOTLY DEBATED: lots of Reddit activity, moderate friction
    WHEN reddit_volume_percentile >= 0.6
     AND subreddit_spread >= 3
      THEN 'hotly_debated'

    -- EMERGING FRICTION: rising search interest + some wiki lookups
    WHEN trends_percentile >= 0.6
     AND trends_lifecycle IN ('surging', 'emerging')
      THEN 'emerging_friction'

    -- LOW FRICTION: working as intended
    WHEN friction_score < 0.3
      THEN 'low_friction'

    ELSE 'moderate_friction'
  END AS friction_class,

  -- Detail signals
  fandom_views,
  fandom_edits,
  fandom_edits_7d,
  view_to_edit_ratio,
  edit_velocity_7d,
  lore_depth_class,

  reddit_volume,
  reddit_sentiment,
  reddit_sentiment_7d,
  reddit_heat_profile,
  subreddit_spread,
  viral_events_30d,

  trends_interest,
  trends_lifecycle,

  -- Normalized scores (for sorting/filtering)
  fandom_view_percentile,
  fandom_edit_percentile,
  reddit_volume_percentile,
  reddit_negativity_percentile,
  trends_percentile,

  -- Data confidence
  (has_fandom + has_reddit + has_trends) AS sources_present,
  CASE
    WHEN (has_fandom + has_reddit + has_trends) = 3 THEN 'HIGH'
    WHEN (has_fandom + has_reddit + has_trends) = 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS confidence,

  -- Rank by friction
  ROW_NUMBER() OVER (ORDER BY friction_score DESC) AS rank_overall,
  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY friction_score DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM combined;
