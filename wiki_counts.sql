SELECT category, count(*) as count FROM `dnd-trends-index.gold_data.view_wikipedia_leaderboards` GROUP BY 1 ORDER BY count ASC;
