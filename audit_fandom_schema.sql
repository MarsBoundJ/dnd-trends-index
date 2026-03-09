-- Get Schema
SELECT column_name, data_type 
FROM `dnd-trends-index.dnd_trends_raw.INFORMATION_SCHEMA.COLUMNS` 
WHERE table_name = 'fandom_daily_metrics';

-- Get Sample Record
SELECT * 
FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics` 
LIMIT 1;
