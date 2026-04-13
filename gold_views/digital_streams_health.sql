-- Digital Streams Health Dashboard
-- Dataset: gold_data.digital_streams_health
-- Shows latest data from each digital stream for operational monitoring
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.digital_streams_health`

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.digital_streams_health` AS

WITH steam_latest AS (
  SELECT 'Steam Players' as stream, MAX(snapshot_date) as last_data,
    STRING_AGG(DISTINCT app_name, ', ') as details,
    SUM(concurrent_players) as latest_value
  FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts`)
),
modio_latest AS (
  SELECT 'mod.io Mods' as stream, MAX(fetch_date) as last_data,
    CAST(COUNT(*) AS STRING) as details,
    SUM(downloads_total) as latest_value
  FROM `dnd-trends-index.dnd_trends_raw.modio_mods`
  WHERE fetch_date = (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.modio_mods`)
),
nexus_latest AS (
  SELECT 'Nexus Mods' as stream, MAX(fetch_date) as last_data,
    CAST(COUNT(*) AS STRING) as details,
    SUM(COALESCE(downloads_total, 0)) as latest_value
  FROM `dnd-trends-index.dnd_trends_raw.nexus_mods`
  WHERE fetch_date = (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.nexus_mods`)
),
twitch_latest AS (
  SELECT 'Twitch Viewership' as stream, MAX(snapshot_date) as last_data,
    STRING_AGG(DISTINCT category_name, ', ') as details,
    SUM(total_viewers) as latest_value
  FROM `dnd-trends-index.dnd_trends_raw.twitch_viewership`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `dnd-trends-index.dnd_trends_raw.twitch_viewership`)
),
ao3_latest AS (
  SELECT 'AO3 Fanfiction' as stream, MAX(fetch_date) as last_data,
    CAST(COUNT(DISTINCT tag_name) AS STRING) as details,
    SUM(work_count) as latest_value
  FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`
  WHERE fetch_date = (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`)
),
ddb_latest AS (
  SELECT 'DDB Catalog' as stream, MAX(fetch_date) as last_data,
    CAST(COUNT(*) AS STRING) as details,
    CAST(SUM(CASE WHEN is_featured THEN 1 ELSE 0 END) AS INT64) as latest_value
  FROM `dnd-trends-index.dnd_trends_raw.dndbeyond_catalog`
  WHERE fetch_date = (SELECT MAX(fetch_date) FROM `dnd-trends-index.dnd_trends_raw.dndbeyond_catalog`)
)

SELECT * FROM steam_latest
UNION ALL SELECT * FROM modio_latest
UNION ALL SELECT * FROM nexus_latest
UNION ALL SELECT * FROM twitch_latest
UNION ALL SELECT * FROM ao3_latest
UNION ALL SELECT * FROM ddb_latest;
