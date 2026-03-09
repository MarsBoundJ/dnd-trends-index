SELECT category, COUNT(*) as count
FROM `dnd-trends-index.gold_data.view_api_leaderboards`
GROUP BY 1
ORDER BY count DESC;
