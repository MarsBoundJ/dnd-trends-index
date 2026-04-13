-- Fandom Wiki Analytics: The DM Prep Radar
-- Dataset: gold_data.analytics_fandom
-- Per-concept wiki engagement analysis with edit velocity, cross-wiki hotspots, and lore depth
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_fandom`
--        WHERE lore_depth_class = 'evergreen_anchor' ORDER BY edit_velocity_7d DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_fandom` AS

WITH -- Last 7 days aggregate per article per wiki
recent_7d AS (
  SELECT
    article_title,
    wiki_slug,
    SUM(COALESCE(edit_count, 0)) as edits_7d,
    SUM(COALESCE(view_count, 0)) as views_7d,
    AVG(hype_score) as avg_hype_7d,
    COUNT(*) as snapshots_7d,
    MIN(rank_position) as best_rank_7d
  FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY article_title, wiki_slug
),

-- Previous 7 days (8-14 days ago)
prev_7d AS (
  SELECT
    article_title,
    wiki_slug,
    SUM(COALESCE(edit_count, 0)) as edits_prev7d,
    SUM(COALESCE(view_count, 0)) as views_prev7d,
    AVG(hype_score) as avg_hype_prev7d
  FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    AND extraction_date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY article_title, wiki_slug
),

-- 30-day aggregate per article per wiki
full_30d AS (
  SELECT
    article_title,
    wiki_slug,
    SUM(COALESCE(edit_count, 0)) as edits_30d,
    SUM(COALESCE(view_count, 0)) as views_30d,
    AVG(hype_score) as avg_hype_30d,
    MAX(hype_score) as max_hype_30d,
    STDDEV(hype_score) as stddev_hype,
    COUNT(DISTINCT extraction_date) as active_days_30d,
    MIN(rank_position) as best_rank_30d,
    MAX(extraction_date) as last_seen
  FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
  WHERE extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY article_title, wiki_slug
),

-- Cross-wiki detection: same article trending on multiple wikis
cross_wiki AS (
  SELECT
    article_title,
    COUNT(DISTINCT wiki_slug) as wiki_count,
    STRING_AGG(DISTINCT wiki_slug, ', ' ORDER BY wiki_slug) as active_wikis
  FROM full_30d
  WHERE avg_hype_30d > 0 OR edits_30d > 0
  GROUP BY article_title
),

-- Concept mapping: join to concept library via fandom mapping
mapped AS (
  SELECT
    fm.canonical_concept,
    fm.category,
    f.article_title,
    f.wiki_slug
  FROM `dnd-trends-index.silver_data.view_fandom_mapping` fm
  JOIN full_30d f ON LOWER(fm.canonical_concept) = LOWER(f.article_title)
),

-- Assemble per article+wiki (then we can also aggregate to concept level)
assembled AS (
  SELECT
    f.article_title as concept_name,
    COALESCE(m.category, 'Unmapped') as category,
    f.wiki_slug,

    -- Current metrics
    ROUND(f.avg_hype_30d, 4) as hype_score,
    f.edits_30d,
    f.views_30d,
    f.best_rank_30d,
    f.active_days_30d,
    f.last_seen,

    -- 7-day metrics
    COALESCE(r.edits_7d, 0) as edits_7d,
    COALESCE(r.views_7d, 0) as views_7d,
    ROUND(COALESCE(r.avg_hype_7d, 0), 4) as hype_7d,
    r.best_rank_7d,

    -- Edit velocity: week-over-week change
    ROUND(
      SAFE_DIVIDE(COALESCE(r.edits_7d, 0) - COALESCE(p.edits_prev7d, 0),
                  GREATEST(COALESCE(p.edits_prev7d, 0), 1)),
      3
    ) as edit_velocity_7d,

    -- Hype momentum
    ROUND(
      SAFE_DIVIDE(COALESCE(r.avg_hype_7d, 0) - COALESCE(p.avg_hype_prev7d, 0),
                  GREATEST(COALESCE(p.avg_hype_prev7d, 0), 0.01)),
      3
    ) as hype_momentum,

    -- View-to-edit ratio
    ROUND(
      SAFE_DIVIDE(f.views_30d, GREATEST(f.edits_30d, 1)),
      1
    ) as view_to_edit_ratio,

    -- Cross-wiki signal
    COALESCE(cw.wiki_count, 1) as cross_wiki_count,
    cw.active_wikis,

    -- Lore depth classification
    CASE
      -- Evergreen anchor: consistently active, steady hype, multiple snapshots
      WHEN f.active_days_30d >= 10
           AND f.avg_hype_30d > 0.1
           AND COALESCE(f.stddev_hype, 0) < f.avg_hype_30d * 2
        THEN 'evergreen_anchor'
      -- Active revision: high recent edits, moderate hype
      WHEN COALESCE(r.edits_7d, 0) >= 3 AND f.avg_hype_30d > 0
        THEN 'active_revision'
      -- Flash spike: appeared briefly with high hype then dropped
      WHEN f.max_hype_30d > 0.5 AND f.active_days_30d <= 5
        THEN 'flash_spike'
      -- Forgotten stub: exists but barely any activity
      WHEN f.avg_hype_30d < 0.05 AND f.edits_30d <= 1
        THEN 'forgotten_stub'
      -- Seasonal: periodic activity
      WHEN f.active_days_30d >= 5 AND COALESCE(f.stddev_hype, 0) > f.avg_hype_30d
        THEN 'seasonal_interest'
      -- Default
      ELSE 'moderate_activity'
    END as lore_depth_class,

    -- Rank within wiki
    ROW_NUMBER() OVER (
      PARTITION BY f.wiki_slug
      ORDER BY f.avg_hype_30d DESC, f.edits_30d DESC
    ) as rank_in_wiki,

    -- Rank within category (across all wikis)
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(m.category, 'Unmapped')
      ORDER BY f.avg_hype_30d DESC, f.edits_30d DESC
    ) as rank_in_category,

    -- Standardized output contract
    'lore_engagement' as signal_type,
    ROUND(CAST(f.avg_hype_30d AS FLOAT64) * 100, 1) as primary_metric,
    ROUND(
      SAFE_DIVIDE(COALESCE(r.avg_hype_7d, 0) - COALESCE(p.avg_hype_prev7d, 0),
                  GREATEST(COALESCE(p.avg_hype_prev7d, 0), 0.01)),
      3
    ) as momentum,
    CASE
      WHEN f.active_days_30d >= 14 THEN 'HIGH'
      WHEN f.active_days_30d >= 7 THEN 'MEDIUM'
      ELSE 'LOW'
    END as confidence,
    'fandom_wiki' as stream_name,
    CURRENT_DATE() as snapshot_date

  FROM full_30d f
  LEFT JOIN recent_7d r ON f.article_title = r.article_title AND f.wiki_slug = r.wiki_slug
  LEFT JOIN prev_7d p ON f.article_title = p.article_title AND f.wiki_slug = p.wiki_slug
  LEFT JOIN cross_wiki cw ON f.article_title = cw.article_title
  LEFT JOIN (
    -- Deduplicate: if multiple concept mappings exist, pick first
    SELECT canonical_concept, category, article_title,
      ROW_NUMBER() OVER (PARTITION BY article_title ORDER BY canonical_concept) as rn
    FROM mapped
  ) m ON f.article_title = m.article_title AND m.rn = 1
)

SELECT * FROM assembled;
