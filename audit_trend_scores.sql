SELECT category, COUNT(*) as count
FROM `dnd-trends-index.gold_data.trend_scores`
GROUP BY 1
ORDER BY count DESC;
