SELECT category, count(*) as count
FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
WHERE is_active = TRUE
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20
