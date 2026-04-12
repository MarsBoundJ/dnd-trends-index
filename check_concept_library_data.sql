SELECT 
    concept_name,
    category,
    ruleset
FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
WHERE is_active = TRUE
LIMIT 20
