SELECT 
    wiki_slug, 
    COUNT(*) as total_records,
    MIN(extraction_date) as start_date,
    MAX(extraction_date) as end_date
FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
GROUP BY 1
ORDER BY total_records DESC;
