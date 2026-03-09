SELECT table_schema, table_name 
FROM `dnd-trends-index.region-us.INFORMATION_SCHEMA.TABLES` 
WHERE table_name LIKE '%reddit%';
