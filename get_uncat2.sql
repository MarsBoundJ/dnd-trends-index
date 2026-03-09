SELECT name, sum(score) as total_score 
FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics` 
WHERE category IS NULL OR category = 'Uncategorized' OR category = 'Other'
GROUP BY name 
ORDER BY total_score DESC 
LIMIT 50
