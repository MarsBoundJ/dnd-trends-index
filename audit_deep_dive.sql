-- Check score distribution for Monsters
SELECT 
    google_score_avg, 
    count(*) as count
FROM `dnd-trends-index.gold_data.deep_dive_metrics`
WHERE category = 'Monster'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20;

-- Check latest dates per category (maybe they don't share a MAX date)
SELECT 
    category, 
    MAX(date) as latest_date,
    COUNT(*) as record_count
FROM `dnd-trends-index.gold_data.deep_dive_metrics`
GROUP BY 1
ORDER BY latest_date DESC;

-- Sample check for Spells
SELECT * 
FROM `dnd-trends-index.gold_data.deep_dive_metrics`
WHERE category LIKE '%Spell%'
LIMIT 10;
