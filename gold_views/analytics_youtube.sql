-- YouTube Analytics: The Creator Investment Tracker
-- Dataset: gold_data.analytics_youtube
-- Per-concept creator coverage analysis with diversity, consensus, and sentiment
--
-- Data path: yt_video_intelligence → yt_video_index → channel_registry
-- (youtube_videos is a separate stats table with different video_ids)
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_youtube`
--        WHERE creator_count >= 3 ORDER BY consensus_score DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_youtube` AS

WITH -- Intelligence-based concept signals (Gemini-analyzed)
intel_concepts AS (
  SELECT
    i.concept_name,
    i.category,
    i.video_id,
    i.sentiment_label,
    i.confidence,
    i.verdict
  FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence` i
  WHERE i.confidence >= 0.5  -- filter low-confidence mentions
),

-- Join to video index for channel info and recency
intel_with_video AS (
  SELECT
    ic.concept_name,
    ic.category,
    ic.video_id,
    ic.sentiment_label,
    ic.confidence,
    cr.channel_name,
    cr.tier as channel_tier,
    idx.published_at,
    idx.title as video_title
  FROM intel_concepts ic
  JOIN `dnd-trends-index.dnd_trends_raw.yt_video_index` idx ON ic.video_id = idx.video_id
  JOIN `dnd-trends-index.dnd_trends_raw.channel_registry` cr ON idx.channel_id = cr.channel_id
),

-- Per-concept aggregation
concept_agg AS (
  SELECT
    concept_name,
    category,
    COUNT(DISTINCT video_id) as video_count,
    COUNT(DISTINCT channel_name) as creator_count,
    -- Sentiment breakdown
    COUNTIF(sentiment_label = 'Positive') as positive_count,
    COUNTIF(sentiment_label = 'Negative') as negative_count,
    COUNTIF(sentiment_label = 'Neutral') as neutral_count,
    COUNTIF(sentiment_label = 'Mixed') as mixed_count,
    -- Recency: how many videos in last 30 days
    COUNTIF(published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) as videos_30d,
    COUNTIF(published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) as videos_7d,
    -- Channel tier distribution
    COUNTIF(channel_tier = 1) as tier1_creators,
    COUNTIF(channel_tier = 2) as tier2_creators,
    -- Avg confidence of concept identification
    AVG(confidence) as avg_confidence,
    -- Creator list
    STRING_AGG(DISTINCT channel_name, ', ' ORDER BY channel_name) as creator_list
  FROM intel_with_video
  GROUP BY concept_name, category
),

-- Keyword-matched signals (from youtube_videos.matched_keywords — separate video pool)
keyword_signals AS (
  SELECT
    kw as concept_name,
    COUNT(DISTINCT video_id) as kw_video_count,
    COUNT(DISTINCT channel_name) as kw_creator_count
  FROM `dnd-trends-index.dnd_trends_raw.youtube_videos`,
    UNNEST(matched_keywords) as kw
  WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY kw
)

SELECT
  c.concept_name,
  c.category,

  -- Creator investment metrics
  c.creator_count,
  c.video_count,
  c.videos_30d,
  c.videos_7d,
  c.tier1_creators,
  c.tier2_creators,

  -- Consensus score: creator_count * recency_factor
  -- Multiple independent creators covering same topic = strong signal
  ROUND(
    c.creator_count * (1.0 + SAFE_DIVIDE(c.videos_30d, GREATEST(c.video_count, 1))),
    2
  ) as consensus_score,

  -- Upload cadence: videos per week (last 30 days)
  ROUND(SAFE_DIVIDE(c.videos_30d, 4.3), 2) as upload_cadence_weekly,

  -- Sentiment analysis
  c.positive_count,
  c.negative_count,
  c.neutral_count,
  c.mixed_count,
  -- Dominant sentiment
  CASE
    WHEN c.positive_count > c.negative_count + c.mixed_count THEN 'positive'
    WHEN c.negative_count > c.positive_count THEN 'negative'
    WHEN c.mixed_count > c.positive_count AND c.mixed_count > c.negative_count THEN 'mixed'
    ELSE 'neutral'
  END as dominant_sentiment,
  -- Sentiment divergence: are creators split?
  CASE
    WHEN c.creator_count >= 2
         AND c.positive_count > 0
         AND c.negative_count > 0
      THEN TRUE
    ELSE FALSE
  END as has_sentiment_divergence,

  -- Keyword match reinforcement (from separate youtube_videos table)
  COALESCE(k.kw_video_count, 0) as keyword_match_videos,
  COALESCE(k.kw_creator_count, 0) as keyword_match_creators,

  -- Creator list for drill-down
  c.creator_list,

  -- Trending indicator
  CASE
    WHEN c.videos_7d >= 2 AND c.creator_count >= 2 THEN TRUE
    ELSE FALSE
  END as is_trending_up,

  -- Rank within category
  ROW_NUMBER() OVER (
    PARTITION BY c.category
    ORDER BY c.creator_count DESC, c.videos_30d DESC
  ) as rank_in_category,

  -- Standardized output contract
  'creator_investment' as signal_type,
  CAST(c.creator_count AS FLOAT64) as primary_metric,
  ROUND(SAFE_DIVIDE(c.videos_7d - (c.videos_30d - c.videos_7d) / 3.3,
                    GREATEST((c.videos_30d - c.videos_7d) / 3.3, 1)), 3) as momentum,
  CASE
    WHEN c.video_count >= 5 AND c.creator_count >= 3 THEN 'HIGH'
    WHEN c.video_count >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'youtube' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM concept_agg c
LEFT JOIN keyword_signals k ON LOWER(c.concept_name) = LOWER(k.concept_name);
