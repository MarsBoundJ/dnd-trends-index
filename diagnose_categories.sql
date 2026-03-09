-- 1. Categories in final API view
SELECT category, COUNT(*) as count 
FROM `dnd-trends-index.gold_data.view_api_leaderboards` 
GROUP BY category 
ORDER BY count DESC;

-- 2. Categories in Concept Library
SELECT category, COUNT(*) as count 
FROM `dnd-trends-index.dnd_trends_categorized.concept_library` 
WHERE is_active = TRUE 
GROUP BY category 
ORDER BY count DESC;

-- 3. Check if 'Mechanic' or 'Class' terms exist in pilot data
SELECT t.search_term, c.category, t.interest
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON t.search_term = c.concept_name
WHERE c.category IN ('Mechanic', 'Class');

-- 4. Terms in pilot data without a category (Null category)
SELECT t.search_term, COUNT(*) as count
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON t.search_term = c.concept_name
WHERE c.category IS NULL
GROUP BY 1
ORDER BY count DESC
LIMIT 10;
