
-- Check for potential data issues:
-- 1. Terms with ALL zeros (failed fetching? or just niche?)
-- 2. Terms with valid data (to prove the pipeline works)

WITH term_stats AS (
    SELECT 
        search_term,
        COUNT(*) as total_weeks,
        SUM(interest) as total_interest,
        AVG(interest) as avg_interest,
        MAX(interest) as max_interest,
        MIN(interest) as min_interest
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
    GROUP BY 1
)

SELECT * FROM term_stats
ORDER BY total_interest DESC
LIMIT 20;
