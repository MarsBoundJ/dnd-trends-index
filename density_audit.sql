SELECT 
    category, 
    date, 
    COUNT(*) as item_count, 
    AVG(google_score_avg) as avg_score,
    MAX(google_score_avg) as max_score
FROM `dnd-trends-index.gold_data.deep_dive_metrics`
GROUP BY 1, 2
ORDER BY category, date DESC;
