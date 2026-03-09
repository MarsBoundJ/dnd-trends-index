-- Distribution of Max Scores
SELECT 
  interest_bucket, 
  COUNT(*) as term_count 
FROM (
  SELECT MAX(interest) as interest_bucket 
  FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` 
  GROUP BY search_term
)
GROUP BY 1 
ORDER BY 1 DESC;

-- Sample of terms with average vs max
SELECT 
    search_term, 
    AVG(interest) as avg_score, 
    MAX(interest) as max_score,
    COUNT(*) as points
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
GROUP BY 1
ORDER BY avg_score DESC
LIMIT 20;
