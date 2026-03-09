SELECT name, sum(metric_score) as total_score 
FROM `dnd-trends-index.gold_data.view_social_leaderboards` 
WHERE source = 'reddit' AND (category IS NULL OR category = 'Uncategorized' OR category = 'Other') 
GROUP BY name 
ORDER BY total_score DESC 
LIMIT 50
