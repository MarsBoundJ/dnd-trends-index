-- Cross-Pollination Index: Digital Footprint View
-- Dataset: gold_data.cross_pollination_digital
-- Aggregates all digital stream signals (AO3, Steam, Twitch, mod.io, Nexus, YouTube Shorts)
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.cross_pollination_digital`

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.cross_pollination_digital` AS

WITH ao3_latest AS (
  SELECT tag_name, tag_type, work_count,
    ROW_NUMBER() OVER (PARTITION BY tag_name ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.ao3_tag_counts`
),
ao3 AS (
  SELECT tag_name, tag_type, work_count FROM ao3_latest WHERE rn = 1
),

twitch_latest AS (
  SELECT category_name, total_viewers, total_streams, top_tags, snapshot_date,
    ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY snapshot_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.twitch_viewership`
),
twitch AS (
  SELECT category_name, total_viewers, total_streams, top_tags
  FROM twitch_latest WHERE rn = 1
),

steam_latest AS (
  SELECT app_name, concurrent_players, snapshot_date,
    ROW_NUMBER() OVER (PARTITION BY app_name ORDER BY snapshot_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.steam_player_counts`
),
steam AS (SELECT app_name, concurrent_players FROM steam_latest WHERE rn = 1),

steam_rev AS (
  SELECT app_name, total_positive, total_negative, review_score_pct,
    ROW_NUMBER() OVER (PARTITION BY app_name ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.steam_reviews`
),
reviews AS (SELECT app_name, total_positive, total_negative, review_score_pct FROM steam_rev WHERE rn = 1),

modio_top AS (
  SELECT mod_name, downloads_total, subscribers_total, tags,
    ROW_NUMBER() OVER (PARTITION BY mod_id ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.modio_mods`
),
modio AS (SELECT mod_name, downloads_total, subscribers_total FROM modio_top WHERE rn = 1),

nexus_top AS (
  SELECT mod_name, downloads_total, endorsement_count,
    ROW_NUMBER() OVER (PARTITION BY mod_id ORDER BY fetch_date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.nexus_mods`
),
nexus AS (SELECT mod_name, downloads_total, endorsement_count FROM nexus_top WHERE rn = 1),

youtube_shorts AS (
  SELECT COUNT(*) as shorts_count, SUM(view_count) as shorts_views
  FROM `dnd-trends-index.dnd_trends_raw.youtube_videos`
  WHERE is_short = TRUE
)

-- AO3 fanfiction signals
SELECT "ao3" as source, a.tag_name as concept_name, a.tag_type as signal_type,
  a.work_count as signal_value, "fanfiction_works" as metric
FROM ao3 a
UNION ALL
-- Twitch viewers
SELECT "twitch", t.category_name, "streaming", t.total_viewers, "live_viewers" FROM twitch t
UNION ALL
-- Twitch streams
SELECT "twitch", t.category_name, "streaming", t.total_streams, "active_streams" FROM twitch t
UNION ALL
-- Steam players
SELECT "steam", s.app_name, "video_game", s.concurrent_players, "concurrent_players" FROM steam s
UNION ALL
-- Steam reviews
SELECT "steam", r.app_name, "video_game", r.total_positive, "positive_reviews" FROM reviews r
UNION ALL
-- mod.io aggregate
SELECT "modio", "BG3 Mods (mod.io)", "modding", SUM(m.downloads_total), "total_mod_downloads" FROM modio m
UNION ALL
-- Nexus aggregate
SELECT "nexus", "BG3 Mods (Nexus)", "modding", SUM(n.downloads_total), "total_mod_downloads" FROM nexus n
UNION ALL
-- YouTube Shorts
SELECT "youtube", "D&D YouTube Shorts", "short_form_video", CAST(ys.shorts_count AS INT64), "shorts_count"
FROM youtube_shorts ys;
