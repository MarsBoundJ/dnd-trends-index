SELECT category, count(*) as c FROM `dnd-trends-index.gold_data.view_social_leaderboards` WHERE source = 'reddit' GROUP BY category ORDER BY c DESC
