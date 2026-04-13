-- Itch.io Analytics: The 18-Month Crystal Ball
-- Dataset: gold_data.analytics_itchio
-- Jam theme heatmap and indie creator trend analysis
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_itchio`
--        WHERE signal_source = 'jam' ORDER BY submission_total DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_itchio` AS

WITH -- Jam analysis: theme keyword frequency weighted by submissions
jam_themes AS (
  SELECT
    kw as theme_keyword,
    COUNT(DISTINCT jam_id) as jam_count,
    SUM(submission_count) as submission_total,
    AVG(submission_count) as avg_submissions_per_jam,
    -- Recency: jams active in last 90 days
    COUNTIF(start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)) as recent_jam_count,
    SUM(CASE WHEN start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        THEN submission_count ELSE 0 END) as recent_submissions
  FROM `dnd-trends-index.dnd_trends_raw.itchio_jams`,
    UNNEST(theme_keywords) as kw
  WHERE submission_count > 0
  GROUP BY kw
),

-- Product tag analysis: what creators are building
product_tags AS (
  SELECT
    tag as product_tag,
    COUNT(DISTINCT product_id) as product_count,
    COUNT(DISTINCT creator) as creator_count,
    AVG(price) as avg_price,
    COUNTIF(is_pwyw = TRUE) as pwyw_count,
    COUNTIF(price > 0 AND is_pwyw = FALSE) as paid_count
  FROM `dnd-trends-index.dnd_trends_raw.itchio_products`,
    UNNEST(tags) as tag
  GROUP BY tag
),

-- Aesthetic cluster analysis
cluster_signals AS (
  SELECT
    cluster as aesthetic_cluster,
    COUNT(DISTINCT product_id) as product_count,
    COUNT(DISTINCT creator) as creator_count
  FROM `dnd-trends-index.dnd_trends_raw.itchio_products`,
    UNNEST(aesthetic_clusters) as cluster
  GROUP BY cluster
)

-- Jam theme signals
SELECT
  jt.theme_keyword as concept_name,
  'Jam Theme' as category,
  'jam' as signal_source,

  jt.jam_count,
  jt.submission_total,
  ROUND(jt.avg_submissions_per_jam, 0) as avg_submissions,
  jt.recent_jam_count,
  jt.recent_submissions,

  -- Trend direction: are recent jams more active than historical?
  CASE
    WHEN jt.recent_jam_count > 0 AND SAFE_DIVIDE(jt.recent_submissions, GREATEST(jt.recent_jam_count, 1))
         > jt.avg_submissions_per_jam * 1.2
      THEN 'accelerating'
    WHEN jt.recent_jam_count > 0
      THEN 'active'
    ELSE 'historical'
  END as trend_direction,

  CAST(NULL AS INT64) as product_count,
  CAST(NULL AS INT64) as creator_count,
  CAST(NULL AS FLOAT64) as avg_price,
  CAST(NULL AS FLOAT64) as pwyw_ratio,

  -- Standardized output contract
  'creator_intent' as signal_type,
  CAST(jt.submission_total AS FLOAT64) as primary_metric,
  ROUND(SAFE_DIVIDE(jt.recent_submissions - (jt.submission_total - jt.recent_submissions),
                    GREATEST(jt.submission_total - jt.recent_submissions, 1)), 3) as momentum,
  CASE
    WHEN jt.jam_count >= 5 THEN 'HIGH'
    WHEN jt.jam_count >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'itchio' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM jam_themes jt
WHERE jt.submission_total >= 10  -- filter noise

UNION ALL

-- Product tag signals
SELECT
  pt.product_tag as concept_name,
  'Product Tag' as category,
  'product' as signal_source,

  CAST(NULL AS INT64) as jam_count,
  CAST(NULL AS INT64) as submission_total,
  CAST(NULL AS INT64) as avg_submissions,
  CAST(NULL AS INT64) as recent_jam_count,
  CAST(NULL AS INT64) as recent_submissions,
  CAST(NULL AS STRING) as trend_direction,

  pt.product_count,
  pt.creator_count,
  ROUND(pt.avg_price, 2) as avg_price,
  ROUND(SAFE_DIVIDE(pt.pwyw_count, GREATEST(pt.product_count, 1)), 3) as pwyw_ratio,

  'creator_intent' as signal_type,
  CAST(pt.product_count AS FLOAT64) as primary_metric,
  CAST(NULL AS FLOAT64) as momentum,
  CASE
    WHEN pt.product_count >= 5 THEN 'HIGH'
    WHEN pt.product_count >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'itchio' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM product_tags pt
WHERE pt.product_count >= 2  -- filter single-product tags

UNION ALL

-- Aesthetic cluster signals
SELECT
  cs.aesthetic_cluster as concept_name,
  'Aesthetic Cluster' as category,
  'cluster' as signal_source,

  CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS INT64),
  CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS STRING),

  cs.product_count,
  cs.creator_count,
  CAST(NULL AS FLOAT64) as avg_price,
  CAST(NULL AS FLOAT64) as pwyw_ratio,

  'creator_intent' as signal_type,
  CAST(cs.product_count AS FLOAT64) as primary_metric,
  CAST(NULL AS FLOAT64) as momentum,
  CASE
    WHEN cs.product_count >= 5 THEN 'HIGH'
    WHEN cs.product_count >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'itchio' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM cluster_signals cs
WHERE cs.product_count >= 2;
