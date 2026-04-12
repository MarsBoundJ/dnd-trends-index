SELECT column_name, data_type, is_nullable
FROM `dnd-trends-index.dnd_trends_categorized.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'concept_library'
ORDER BY ordinal_position
