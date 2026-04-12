SELECT 
    concept_name,
    category,
    ruleset
FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
WHERE is_active = TRUE
AND category IN ('Class', 'Subclass', 'Spell', 'Monster', 'Feat', 'Species & Lineage', 'Species')
LIMIT 20
