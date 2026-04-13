-- Reddit Analytics: The Community Heat Engine
-- Dataset: gold_data.analytics_reddit
-- Per-concept community engagement analysis with heat profiles, sentiment, and virality
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_reddit`
--        WHERE heat_profile = 'controversial' ORDER BY mention_count_30d DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_reddit` AS

WITH -- 30-day aggregate per concept across all subreddits
concept_30d AS (
  SELECT
    keyword as concept_name,
    category,
    SUM(mention_count) as mention_count_30d,
    AVG(weighted_score) as avg_weighted_score,
    COUNT(DISTINCT subreddit) as subreddit_spread,
    COUNT(DISTINCT extraction_date) as active_days,
    MAX(mention_count) as peak_daily_mentions
  FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND mention_count > 0
  GROUP BY keyword, category
),

-- Per-subreddit breakdown (for identifying dominant subs)
concept_by_sub AS (
  SELECT
    keyword as concept_name,
    category,
    subreddit,
    SUM(mention_count) as sub_mentions,
    AVG(weighted_score) as sub_avg_score
  FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND mention_count > 0
  GROUP BY keyword, category, subreddit
),

-- Find top subreddit per concept
top_sub AS (
  SELECT concept_name, category, subreddit as top_subreddit, sub_mentions as top_sub_mentions,
    ROW_NUMBER() OVER (PARTITION BY concept_name, category ORDER BY sub_mentions DESC) as rn
  FROM concept_by_sub
),

-- Subreddit average daily posts (for normalization denominator)
sub_baselines AS (
  SELECT subreddit,
    AVG(daily_total) as avg_daily_posts
  FROM (
    SELECT subreddit, extraction_date, SUM(mention_count) as daily_total
    FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
    WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY subreddit, extraction_date
  )
  GROUP BY subreddit
),

-- Normalized volume: concept mentions / subreddit average
normalized AS (
  SELECT
    cs.concept_name, cs.category, cs.subreddit,
    SAFE_DIVIDE(cs.sub_mentions, GREATEST(sb.avg_daily_posts, 1)) as normalized_volume
  FROM concept_by_sub cs
  JOIN sub_baselines sb ON cs.subreddit = sb.subreddit
),

-- Max normalized volume per concept (across subs)
max_norm AS (
  SELECT concept_name, category, MAX(normalized_volume) as max_normalized_volume
  FROM normalized
  GROUP BY concept_name, category
),

-- Momentum: compare last 7 days to previous 7 days
recent_7d AS (
  SELECT keyword as concept_name, category,
    SUM(mention_count) as mentions_7d,
    AVG(weighted_score) as sentiment_7d
  FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND mention_count > 0
  GROUP BY keyword, category
),

prev_7d AS (
  SELECT keyword as concept_name, category,
    SUM(mention_count) as mentions_prev7d
  FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    AND extraction_date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND mention_count > 0
  GROUP BY keyword, category
),

-- Viral events per concept (last 30 days)
viral AS (
  SELECT topic as concept_name,
    COUNT(*) as viral_events_30d,
    MAX(upvotes) as max_viral_upvotes,
    STRING_AGG(DISTINCT sentiment, ', ') as viral_sentiments
  FROM `dnd-trends-index.dnd_trends_categorized.reddit_viral_events`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY topic
)

SELECT
  c.concept_name,
  c.category,

  -- Volume metrics
  c.mention_count_30d,
  c.peak_daily_mentions,
  c.active_days,
  ROUND(COALESCE(mn.max_normalized_volume, 0), 3) as normalized_volume,

  -- Sentiment
  ROUND(c.avg_weighted_score, 2) as sentiment_avg,
  ROUND(COALESCE(r7.sentiment_7d, 0), 2) as sentiment_7d,

  -- Spread
  c.subreddit_spread,
  ts.top_subreddit,

  -- Virality
  COALESCE(v.viral_events_30d, 0) as viral_events_30d,
  v.max_viral_upvotes,

  -- Momentum
  ROUND(
    SAFE_DIVIDE(COALESCE(r7.mentions_7d, 0) - COALESCE(p7.mentions_prev7d, 0),
                GREATEST(COALESCE(p7.mentions_prev7d, 0), 1)),
    3
  ) as momentum_7d,

  -- Community Heat Profile classification
  CASE
    -- Controversial: high volume + mixed/negative sentiment
    WHEN c.mention_count_30d >= 5 AND c.avg_weighted_score < 10
      THEN 'controversial'
    -- Loud niche: high volume but concentrated in 1-2 subs
    WHEN c.mention_count_30d >= 10 AND c.subreddit_spread <= 2
      THEN 'loud_niche'
    -- Broad casual: moderate mentions across 4+ subs
    WHEN c.mention_count_30d >= 3 AND c.subreddit_spread >= 4
      THEN 'broad_casual'
    -- Viral burst: has viral events but may not have sustained mentions
    WHEN COALESCE(v.viral_events_30d, 0) > 0
      THEN 'viral_burst'
    -- Quiet but present: steady low-level mentions
    WHEN c.mention_count_30d >= 2 AND c.active_days >= 7
      THEN 'quiet_steady'
    -- Default: minimal signal
    ELSE 'minimal'
  END as heat_profile,

  -- Rank within category
  ROW_NUMBER() OVER (
    PARTITION BY c.category
    ORDER BY c.mention_count_30d DESC
  ) as rank_in_category,

  -- Standardized output contract
  'engagement' as signal_type,
  CAST(c.mention_count_30d AS FLOAT64) as primary_metric,
  ROUND(
    SAFE_DIVIDE(COALESCE(r7.mentions_7d, 0) - COALESCE(p7.mentions_prev7d, 0),
                GREATEST(COALESCE(p7.mentions_prev7d, 0), 1)),
    3
  ) as momentum,
  CASE
    WHEN c.active_days >= 14 THEN 'HIGH'
    WHEN c.active_days >= 7 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'reddit' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM concept_30d c
LEFT JOIN (SELECT * FROM top_sub WHERE rn = 1) ts
  ON c.concept_name = ts.concept_name AND c.category = ts.category
LEFT JOIN max_norm mn ON c.concept_name = mn.concept_name AND c.category = mn.category
LEFT JOIN recent_7d r7 ON c.concept_name = r7.concept_name AND c.category = r7.category
LEFT JOIN prev_7d p7 ON c.concept_name = p7.concept_name AND c.category = p7.category
LEFT JOIN viral v ON c.concept_name = v.concept_name;
