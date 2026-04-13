-- Cross-Pollination Index: Master View
-- Dataset: gold_data.cross_pollination_index
-- Joins concept_library with digital and paper footprint signals
-- Core analytical view for the WotC presentation
--
-- Pollination status:
--   cross_pollinated = concept appears in BOTH digital and paper signals
--   digital_only     = only digital signals (BG3 characters, mods, etc.)
--   paper_only       = only paper signals (tabletop-only concepts)
--   no_signal        = in concept library but no recent signal data
--
-- Usage:
--   SELECT * FROM `dnd-trends-index.gold_data.cross_pollination_index`
--   WHERE pollination_status = 'cross_pollinated'
--   ORDER BY digital_raw_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.cross_pollination_index` AS

WITH concept_base AS (
  SELECT concept_name, category, origin, canonical_source, source_book
  FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
  WHERE is_active = TRUE
),

digital_agg AS (
  SELECT concept_name,
    SUM(CASE WHEN metric = 'fanfiction_works' THEN signal_value ELSE 0 END) as ao3_works,
    SUM(CASE WHEN metric = 'live_viewers' THEN signal_value ELSE 0 END) as twitch_viewers,
    SUM(CASE WHEN metric = 'concurrent_players' THEN signal_value ELSE 0 END) as steam_players,
    SUM(CASE WHEN metric = 'positive_reviews' THEN signal_value ELSE 0 END) as steam_positive_reviews,
    COUNT(DISTINCT source) as digital_source_count
  FROM `dnd-trends-index.gold_data.cross_pollination_digital`
  GROUP BY concept_name
),

paper_agg AS (
  SELECT concept_name,
    SUM(CASE WHEN metric = 'wikipedia_views_30d' THEN signal_value ELSE 0 END) as wiki_views_30d,
    SUM(CASE WHEN metric = 'fandom_edits_30d' THEN signal_value ELSE 0 END) as fandom_edits_30d,
    SUM(CASE WHEN metric = 'reddit_mentions_30d' THEN signal_value ELSE 0 END) as reddit_mentions_30d,
    SUM(CASE WHEN metric = 'bgg_owned_count' THEN signal_value ELSE 0 END) as bgg_owned,
    COUNT(DISTINCT source) as paper_source_count
  FROM `dnd-trends-index.gold_data.cross_pollination_paper`
  GROUP BY concept_name
)

SELECT
  c.concept_name, c.category, c.origin, c.canonical_source, c.source_book,
  -- Digital signals
  COALESCE(d.ao3_works, 0) as ao3_works,
  COALESCE(d.twitch_viewers, 0) as twitch_viewers,
  COALESCE(d.steam_players, 0) as steam_players,
  COALESCE(d.steam_positive_reviews, 0) as steam_positive_reviews,
  COALESCE(d.digital_source_count, 0) as digital_source_count,
  -- Paper signals
  COALESCE(p.wiki_views_30d, 0) as wiki_views_30d,
  COALESCE(p.fandom_edits_30d, 0) as fandom_edits_30d,
  COALESCE(p.reddit_mentions_30d, 0) as reddit_mentions_30d,
  COALESCE(p.bgg_owned, 0) as bgg_owned,
  COALESCE(p.paper_source_count, 0) as paper_source_count,
  -- Composite scores
  COALESCE(d.ao3_works, 0) + COALESCE(d.steam_positive_reviews, 0) as digital_raw_score,
  COALESCE(p.wiki_views_30d, 0) + COALESCE(p.reddit_mentions_30d, 0) * 10
    + COALESCE(p.fandom_edits_30d, 0) * 100 as paper_raw_score,
  -- Classification
  CASE
    WHEN COALESCE(d.digital_source_count, 0) > 0 AND COALESCE(p.paper_source_count, 0) > 0
      THEN 'cross_pollinated'
    WHEN COALESCE(d.digital_source_count, 0) > 0 THEN 'digital_only'
    WHEN COALESCE(p.paper_source_count, 0) > 0 THEN 'paper_only'
    ELSE 'no_signal'
  END as pollination_status
FROM concept_base c
LEFT JOIN digital_agg d ON LOWER(c.concept_name) = LOWER(d.concept_name)
LEFT JOIN paper_agg p ON LOWER(c.concept_name) = LOWER(p.concept_name);
