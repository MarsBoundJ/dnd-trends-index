SELECT category, heat_score, COUNT(*) as items 
FROM `dnd-trends-index.gold_data.view_api_leaderboards` 
GROUP BY 1, 2 
ORDER BY heat_score DESC;
