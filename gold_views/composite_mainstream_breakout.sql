-- Mainstream Breakout Tracker: What's Crossing Into Pop Culture?
-- Dataset: gold_data.composite_mainstream_breakout
-- Reads from: analytics_wikipedia, analytics_google_trends, analytics_digital_gaming,
--             composite_concept_index
--
-- Detects D&D concepts breaking out of the hobby niche into mainstream awareness.
-- These are IP opportunities — concepts WotC should capitalize on before they peak.
--
-- Mainstream signals:
--   Wikipedia views = general public (not just D&D players) are reading about this
--   Google Trends breakout = sudden spike in public search interest
--   Twitch/Steam = digital audience exposure (BG3 → mainstream awareness)
--
-- Usage:
--   -- Currently breaking out:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_mainstream_breakout`
--   WHERE breakout_stage = 'breaking_out' ORDER BY breakout_score DESC
--
--   -- Already mainstream:
--   SELECT * FROM `dnd-trends-index.gold_data.composite_mainstream_breakout`
--   WHERE breakout_stage = 'mainstream' ORDER BY mainstream_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.composite_mainstream_breakout` AS

WITH -- Wikipedia: the mainstream awareness gauge
wiki AS (
  SELECT
    concept_name,
    category,
    views_30d,
    views_7d,
    avg_daily_views,
    mainstream_score,
    momentum_ratio_7d AS wiki_momentum_7d,
    momentum_ratio_30d AS wiki_momentum_30d,
    view_pattern,
    is_rising AS wiki_is_rising,
    -- Percentile among all tracked Wikipedia articles
    PERCENT_RANK() OVER (ORDER BY views_30d ASC) AS wiki_percentile
  FROM `dnd-trends-index.gold_data.analytics_wikipedia`
),

-- Google Trends: search interest breakouts
trends AS (
  SELECT
    concept_name,
    category,
    current_interest,
    momentum_14d AS trends_momentum,
    is_breakout AS trends_is_breakout,
    lifecycle_state AS trends_lifecycle,
    percentile_vs_self,
    volatility_cv,
    -- Percentile among all tracked trend concepts
    PERCENT_RANK() OVER (ORDER BY current_interest ASC) AS trends_percentile
  FROM `dnd-trends-index.gold_data.analytics_google_trends`
),

-- Digital gaming: Twitch + Steam exposure (mass audience platforms)
digital AS (
  SELECT
    concept_name,
    primary_metric,
    momentum,
    stream_name,
    PERCENT_RANK() OVER (
      PARTITION BY stream_name
      ORDER BY primary_metric ASC
    ) AS digital_percentile
  FROM `dnd-trends-index.gold_data.analytics_digital_gaming`
  WHERE stream_name IN ('twitch', 'steam')  -- mass audience platforms only
),

-- Aggregate digital signals per concept
digital_agg AS (
  SELECT
    concept_name,
    MAX(CASE WHEN stream_name = 'twitch' THEN primary_metric END) AS twitch_viewers,
    MAX(CASE WHEN stream_name = 'steam' THEN primary_metric END) AS steam_players,
    MAX(CASE WHEN stream_name = 'twitch' THEN digital_percentile END) AS twitch_percentile,
    MAX(CASE WHEN stream_name = 'steam' THEN digital_percentile END) AS steam_percentile,
    MAX(CASE WHEN stream_name = 'twitch' THEN momentum END) AS twitch_momentum,
    MAX(CASE WHEN stream_name = 'steam' THEN momentum END) AS steam_momentum
  FROM digital
  GROUP BY concept_name
),

-- Composite index for context
idx AS (
  SELECT
    concept_name,
    category,
    ruleset,
    demand_score,
    supply_score,
    community_score,
    creator_score,
    trend_level,
    streams_present
  FROM `dnd-trends-index.gold_data.composite_concept_index`
),

-- Combine all mainstream signals
combined AS (
  SELECT
    COALESCE(w.concept_name, t.concept_name, d.concept_name) AS concept_name,
    COALESCE(w.category, t.category) AS category_raw,

    -- Wikipedia signals
    w.views_30d AS wiki_views_30d,
    w.views_7d AS wiki_views_7d,
    w.avg_daily_views AS wiki_avg_daily,
    w.wiki_momentum_7d,
    w.wiki_momentum_30d,
    w.wiki_is_rising,
    w.view_pattern AS wiki_pattern,
    COALESCE(w.wiki_percentile, 0) AS wiki_percentile,

    -- Google Trends signals
    t.current_interest AS trends_interest,
    t.trends_momentum,
    t.trends_is_breakout,
    t.trends_lifecycle,
    t.percentile_vs_self AS trends_vs_self,
    COALESCE(t.trends_percentile, 0) AS trends_percentile,

    -- Digital signals
    d.twitch_viewers,
    d.steam_players,
    COALESCE(d.twitch_percentile, 0) AS twitch_percentile,
    COALESCE(d.steam_percentile, 0) AS steam_percentile,
    d.twitch_momentum,
    d.steam_momentum,

    -- Source flags
    CASE WHEN w.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_wiki,
    CASE WHEN t.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_trends,
    CASE WHEN d.concept_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_digital

  FROM wiki w
  FULL OUTER JOIN trends t ON LOWER(w.concept_name) = LOWER(t.concept_name)
  FULL OUTER JOIN digital_agg d
    ON LOWER(COALESCE(w.concept_name, t.concept_name)) = LOWER(d.concept_name)
),

scored AS (
  SELECT
    c.*,

    -- MAINSTREAM SCORE: weighted combination of mainstream-indicating signals
    -- Wikipedia (50%): the single best mainstream indicator
    -- Google Trends (30%): public search interest
    -- Digital platforms (20%): Twitch/Steam mass audience exposure
    ROUND(
      c.wiki_percentile * 0.50 +
      c.trends_percentile * 0.30 +
      GREATEST(c.twitch_percentile, c.steam_percentile) * 0.20
    , 4) AS mainstream_score,

    -- BREAKOUT SCORE: is this concept crossing over RIGHT NOW?
    -- Combines momentum signals (not absolute level)
    ROUND(
      CASE WHEN c.wiki_is_rising = TRUE THEN 0.35 ELSE 0 END +
      CASE WHEN c.trends_is_breakout = TRUE THEN 0.35 ELSE 0 END +
      CASE WHEN c.trends_lifecycle IN ('surging', 'emerging') THEN 0.15 ELSE 0 END +
      CASE WHEN COALESCE(c.twitch_momentum, 0) > 0.1 THEN 0.15 ELSE 0 END
    , 2) AS breakout_score,

    -- Mainstream source count
    (CASE WHEN c.has_wiki THEN 1 ELSE 0 END +
     CASE WHEN c.has_trends THEN 1 ELSE 0 END +
     CASE WHEN c.has_digital THEN 1 ELSE 0 END) AS mainstream_sources

  FROM combined c
)

SELECT
  s.concept_name,
  COALESCE(idx.category, s.category_raw) AS category,
  idx.ruleset,

  -- Scores
  s.mainstream_score,
  s.breakout_score,

  -- Breakout stage classification
  CASE
    -- MAINSTREAM: consistently high Wikipedia + Trends (already crossed over)
    WHEN s.wiki_percentile >= 0.8
     AND s.trends_percentile >= 0.6
      THEN 'mainstream'

    -- BREAKING OUT: active breakout signals firing
    WHEN s.breakout_score >= 0.5
      THEN 'breaking_out'

    -- NICHE FAMOUS: high in D&D circles but not mainstream
    WHEN s.trends_percentile >= 0.6
     AND s.wiki_percentile < 0.4
      THEN 'niche_famous'

    -- DIGITALLY VISIBLE: known through games/streaming, not search
    WHEN GREATEST(s.twitch_percentile, s.steam_percentile) >= 0.6
     AND s.wiki_percentile < 0.5
      THEN 'digitally_visible'

    -- EMERGING AWARENESS: some mainstream signals appearing
    WHEN s.mainstream_score >= 0.3
     AND s.breakout_score > 0
      THEN 'emerging_awareness'

    ELSE 'niche'
  END AS breakout_stage,

  -- Momentum narrative
  CASE
    WHEN s.wiki_is_rising = TRUE AND s.trends_is_breakout = TRUE
      THEN 'dual_breakout'
    WHEN s.wiki_is_rising = TRUE
      THEN 'wiki_surging'
    WHEN s.trends_is_breakout = TRUE
      THEN 'search_breakout'
    WHEN COALESCE(s.wiki_momentum_7d, 1) > 1.2
      THEN 'wiki_rising'
    WHEN COALESCE(s.trends_momentum, 0) > 0.3
      THEN 'search_rising'
    ELSE 'stable'
  END AS momentum_signal,

  -- Detail: Wikipedia
  s.wiki_views_30d,
  s.wiki_views_7d,
  s.wiki_avg_daily,
  s.wiki_momentum_7d,
  s.wiki_pattern,
  ROUND(s.wiki_percentile, 3) AS wiki_percentile,

  -- Detail: Google Trends
  s.trends_interest,
  s.trends_momentum,
  s.trends_is_breakout,
  s.trends_lifecycle,
  ROUND(s.trends_percentile, 3) AS trends_percentile,

  -- Detail: Digital
  s.twitch_viewers,
  s.steam_players,
  ROUND(s.twitch_percentile, 3) AS twitch_percentile,
  ROUND(s.steam_percentile, 3) AS steam_percentile,

  -- Context from composite index
  idx.demand_score,
  idx.supply_score,
  idx.community_score,
  idx.creator_score,
  idx.trend_level,

  -- Confidence
  s.mainstream_sources,
  CASE
    WHEN s.mainstream_sources = 3 THEN 'HIGH'
    WHEN s.mainstream_sources = 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS confidence,

  -- Ranks
  ROW_NUMBER() OVER (ORDER BY s.mainstream_score DESC) AS rank_mainstream,
  ROW_NUMBER() OVER (ORDER BY s.breakout_score DESC) AS rank_breakout,
  ROW_NUMBER() OVER (
    PARTITION BY COALESCE(idx.category, s.category_raw)
    ORDER BY s.mainstream_score DESC
  ) AS rank_in_category,

  CURRENT_DATE() AS snapshot_date

FROM scored s
LEFT JOIN idx ON LOWER(s.concept_name) = LOWER(idx.concept_name);
