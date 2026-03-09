CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.trend_scores` AS
WITH google_daily AS (
  SELECT 
    keyword, 
    MAX(google_score_avg) as max_google,
    MAX(date) as last_date
  FROM `dnd-trends-index.gold_data.view_api_leaderboards`
  GROUP BY keyword
),
fandom_daily AS (
  SELECT 
    canonical_concept as keyword, 
    MAX(score) as max_fandom
  FROM `dnd-trends-index.gold_data.view_fandom_leaderboards`
  GROUP BY 1
),
wiki_daily AS (
  SELECT 
    canonical_concept as keyword, 
    MAX(score) as max_wiki
  FROM `dnd-trends-index.gold_data.view_wikipedia_leaderboards`
  GROUP BY 1
),
yt_daily AS (
  SELECT 
    keyword, 
    COUNT(*) as video_count,
    AVG(views) as avg_views
  FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence`
  GROUP BY 1
)
SELECT 
  COALESCE(g.keyword, f.keyword, w.keyword, y.keyword) as keyword,
  l.category,
  g.max_google as norm_google,
  f.max_fandom as norm_fandom,
  w.max_wiki as norm_wiki,
  y.video_count as norm_youtube,
  0 as norm_roll20, 
  (COALESCE(g.max_google,0)*0.4 + COALESCE(f.max_fandom,0)*0.2 + COALESCE(w.max_wiki,0)*0.2 + COALESCE(y.video_count,0)*10) as trend_score_raw,
  COALESCE(g.max_google,0) as hype_score,
  COALESCE(y.video_count,0) as play_score,
  COALESCE(g.last_date, CURRENT_DATE()) as date
FROM google_daily g
FULL OUTER JOIN fandom_daily f ON g.keyword = f.keyword
FULL OUTER JOIN wiki_daily w ON COALESCE(g.keyword, f.keyword) = w.keyword
FULL OUTER JOIN yt_daily y ON COALESCE(g.keyword, f.keyword, w.keyword) = y.keyword
LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` l ON COALESCE(g.keyword, f.keyword, w.keyword, y.keyword) = l.concept_name
WHERE COALESCE(g.keyword, f.keyword, w.keyword, y.keyword) IS NOT NULL;
