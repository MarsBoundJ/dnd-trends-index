-- Digital Gaming Analytics: Steam, Modding, Twitch, AO3 Combined
-- Dataset: gold_data.analytics_digital_gaming
-- Unified view of D&D digital/video game ecosystem health and trends
--
-- These streams are tightly coupled (BG3 players → BG3 mods → BG3 streams
-- → BG3 fanfiction) so they're analyzed as one ecosystem.
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_digital_gaming`
--        ORDER BY ecosystem_segment, primary_metric DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_digital_gaming` AS

WITH -- Steam: latest player counts per game
steam_latest AS (
  SELECT app_name, concurrent_players, snapshot_date,
    ROW_NUMBER() OVER (PARTITION BY app_name ORDER BY snapshot_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts`
),
steam_prev AS (
  SELECT app_name, concurrent_players as prev_players, snapshot_date as prev_date,
    ROW_NUMBER() OVER (PARTITION BY app_name ORDER BY snapshot_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts`
  WHERE snapshot_date < (SELECT MAX(snapshot_date) FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts`)
),

-- Steam reviews: latest per game
reviews_latest AS (
  SELECT app_name, total_positive, total_negative, review_score_pct, fetch_date,
    ROW_NUMBER() OVER (PARTITION BY app_name ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.steam_reviews`
),

-- mod.io: latest per mod
modio_latest AS (
  SELECT mod_name, downloads_total, subscribers_total, tags, fetch_date,
    ROW_NUMBER() OVER (PARTITION BY mod_id ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.modio_mods`
),
modio_prev AS (
  SELECT mod_name, downloads_total as prev_downloads,
    ROW_NUMBER() OVER (PARTITION BY mod_id ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.modio_mods`
  WHERE fetch_date < (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.modio_mods`)
),

-- Nexus: latest per mod
nexus_latest AS (
  SELECT mod_name, downloads_total, endorsement_count, fetch_date,
    ROW_NUMBER() OVER (PARTITION BY mod_id ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.nexus_mods`
),
nexus_prev AS (
  SELECT mod_name, downloads_total as prev_downloads,
    ROW_NUMBER() OVER (PARTITION BY mod_id ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.nexus_mods`
  WHERE fetch_date < (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.nexus_mods`)
),

-- Twitch: latest per category
twitch_latest AS (
  SELECT category_name, total_viewers, total_streams, top_tags, snapshot_date,
    ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY snapshot_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.twitch_viewership`
),
twitch_prev AS (
  SELECT category_name, total_viewers as prev_viewers,
    ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY snapshot_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.twitch_viewership`
  WHERE snapshot_date < (SELECT MAX(snapshot_date) FROM `dnd-trends-index.dnd_trends_raw.twitch_viewership`)
),

-- AO3: latest per tag
ao3_latest AS (
  SELECT tag_name, tag_type, work_count, fetch_date,
    ROW_NUMBER() OVER (PARTITION BY tag_name ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`
),
ao3_prev AS (
  SELECT tag_name, work_count as prev_works,
    ROW_NUMBER() OVER (PARTITION BY tag_name ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`
  WHERE fetch_date < (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`)
)

-- === STEAM GAMES ===
SELECT
  s.app_name as concept_name,
  'Video Game' as category,
  'steam_players' as ecosystem_segment,
  s.concurrent_players as metric_value,
  'concurrent_players' as metric_name,
  -- Player trend
  s.concurrent_players - COALESCE(sp.prev_players, 0) as delta,
  -- Review health
  COALESCE(r.review_score_pct, 0) as review_score_pct,
  COALESCE(r.total_positive, 0) as review_positive,
  COALESCE(r.total_negative, 0) as review_negative,
  -- Contract
  'gaming_engagement' as signal_type,
  CAST(s.concurrent_players AS FLOAT64) as primary_metric,
  ROUND(SAFE_DIVIDE(s.concurrent_players - COALESCE(sp.prev_players, s.concurrent_players),
    GREATEST(COALESCE(sp.prev_players, s.concurrent_players), 1)), 3) as momentum,
  'HIGH' as confidence,
  'steam' as stream_name,
  CURRENT_DATE() as snapshot_date
FROM steam_latest s
LEFT JOIN (SELECT * FROM steam_prev WHERE rn = 1) sp ON s.app_name = sp.app_name
LEFT JOIN (SELECT * FROM reviews_latest WHERE rn = 1) r ON s.app_name = r.app_name
WHERE s.rn = 1

UNION ALL

-- === MOD.IO TOP MODS ===
SELECT
  m.mod_name, 'BG3 Mod', 'modio',
  m.downloads_total, 'total_downloads',
  m.downloads_total - COALESCE(mp.prev_downloads, 0),
  CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS INT64),
  'modding_engagement', CAST(m.downloads_total AS FLOAT64),
  ROUND(SAFE_DIVIDE(m.downloads_total - COALESCE(mp.prev_downloads, m.downloads_total),
    GREATEST(COALESCE(mp.prev_downloads, m.downloads_total), 1)), 3),
  'MEDIUM', 'modio', CURRENT_DATE()
FROM modio_latest m
LEFT JOIN (SELECT * FROM modio_prev WHERE rn = 1) mp ON m.mod_name = mp.mod_name
WHERE m.rn = 1

UNION ALL

-- === NEXUS TOP MODS ===
SELECT
  n.mod_name, 'BG3 Mod', 'nexus',
  n.downloads_total, 'total_downloads',
  n.downloads_total - COALESCE(np.prev_downloads, 0),
  CAST(n.endorsement_count AS INT64), CAST(NULL AS INT64), CAST(NULL AS INT64),
  'modding_engagement', CAST(n.downloads_total AS FLOAT64),
  ROUND(SAFE_DIVIDE(n.downloads_total - COALESCE(np.prev_downloads, n.downloads_total),
    GREATEST(COALESCE(np.prev_downloads, n.downloads_total), 1)), 3),
  'MEDIUM', 'nexus', CURRENT_DATE()
FROM nexus_latest n
LEFT JOIN (SELECT * FROM nexus_prev WHERE rn = 1) np ON n.mod_name = np.mod_name
WHERE n.rn = 1

UNION ALL

-- === TWITCH CATEGORIES ===
SELECT
  t.category_name, 'Streaming', 'twitch',
  t.total_viewers, 'live_viewers',
  t.total_viewers - COALESCE(tp.prev_viewers, 0),
  t.total_streams, CAST(NULL AS INT64), CAST(NULL AS INT64),
  'entertainment_engagement', CAST(t.total_viewers AS FLOAT64),
  ROUND(SAFE_DIVIDE(t.total_viewers - COALESCE(tp.prev_viewers, t.total_viewers),
    GREATEST(COALESCE(tp.prev_viewers, t.total_viewers), 1)), 3),
  'HIGH', 'twitch', CURRENT_DATE()
FROM twitch_latest t
LEFT JOIN (SELECT * FROM twitch_prev WHERE rn = 1) tp ON t.category_name = tp.category_name
WHERE t.rn = 1

UNION ALL

-- === AO3 FANFICTION ===
SELECT
  a.tag_name, a.tag_type, 'ao3',
  a.work_count, 'fanfiction_works',
  a.work_count - COALESCE(ap.prev_works, 0),
  CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS INT64),
  'fandom_creation', CAST(a.work_count AS FLOAT64),
  ROUND(SAFE_DIVIDE(a.work_count - COALESCE(ap.prev_works, a.work_count),
    GREATEST(COALESCE(ap.prev_works, a.work_count), 1)), 3),
  CASE WHEN a.work_count >= 1000 THEN 'HIGH'
       WHEN a.work_count >= 100 THEN 'MEDIUM'
       ELSE 'LOW' END,
  'ao3', CURRENT_DATE()
FROM ao3_latest a
LEFT JOIN (SELECT * FROM ao3_prev WHERE rn = 1) ap ON a.tag_name = ap.tag_name
WHERE a.rn = 1;
