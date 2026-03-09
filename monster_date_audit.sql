SELECT date, count(*) as count
FROM `dnd-trends-index.gold_data.deep_dive_metrics` 
WHERE category = 'Monster' 
GROUP BY 1 
ORDER BY 1 DESC 
LIMIT 20;
