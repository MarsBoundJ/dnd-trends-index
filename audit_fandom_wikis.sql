SELECT 
    wiki_id, 
    wiki_domain, 
    wiki_title, 
    project_id,
    MIN(scrape_date) as start_date,
    MAX(scrape_date) as end_date,
    COUNT(*) as total_records
FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
GROUP BY 1, 2, 3, 4
ORDER BY total_records DESC;
