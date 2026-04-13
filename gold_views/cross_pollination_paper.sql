-- Cross-Pollination Index: Paper/Tabletop Footprint View
-- Dataset: gold_data.cross_pollination_paper
-- Aggregates traditional D&D signals (Wikipedia, Fandom wiki, Reddit, BGG)
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.cross_pollination_paper`

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.cross_pollination_paper` AS

WITH wiki_recent AS (
  SELECT article_title, SUM(views) as total_views_30d
  FROM `dnd-trends-index.social_data.wikipedia_daily_views`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY article_title
),

fandom_recent AS (
  SELECT article_title,
    AVG(hype_score) as avg_hype_score,
    SUM(view_count) as total_views,
    SUM(edit_count) as total_edits
  FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY article_title
),

reddit_recent AS (
  SELECT keyword, category,
    SUM(mention_count) as total_mentions,
    SUM(weighted_score) as total_weighted_score
  FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY keyword, category
),

bgg_latest AS (
  SELECT concept_name,
    MAX(owned_count) as owned_count,
    MAX(quality_score) as quality_score
  FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`
  GROUP BY concept_name
)

SELECT "wikipedia" as source, w.article_title as concept_name, "wiki_engagement" as signal_type,
  w.total_views_30d as signal_value, "wikipedia_views_30d" as metric
FROM wiki_recent w WHERE w.total_views_30d > 100
UNION ALL
SELECT "fandom_wiki", f.article_title, "wiki_engagement",
  CAST(f.total_edits AS INT64), "fandom_edits_30d"
FROM fandom_recent f WHERE f.total_edits > 0
UNION ALL
SELECT "reddit", r.keyword, r.category, r.total_mentions, "reddit_mentions_30d"
FROM reddit_recent r WHERE r.total_mentions > 0
UNION ALL
SELECT "bgg", b.concept_name, "tabletop_product", CAST(b.owned_count AS INT64), "bgg_owned_count"
FROM bgg_latest b;
