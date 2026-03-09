SELECT matched_category, COUNT(*) 
FROM `dnd-trends-index.dnd_trends_raw.reddit_daily_metrics` 
WHERE matched_concept IS NOT NULL 
GROUP BY 1;
