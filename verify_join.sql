
SELECT 
  e.search_term, 
  e.expansion_rule, 
  c.concept_name, 
  c.category,
  c.tags
FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` e
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c
ON e.original_keyword = c.concept_name 
AND e.category = c.category
LIMIT 5
