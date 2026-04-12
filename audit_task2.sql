SELECT title, ARRAY_TO_STRING(tags, ', ') as tags_str
FROM `silver_data.norm_catalog_supply` 
WHERE source = 'DMs Guild' 
LIMIT 10;
