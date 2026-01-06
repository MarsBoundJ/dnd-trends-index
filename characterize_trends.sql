SELECT 
    count(*) as total_rows, 
    count(distinct query) as unique_keywords, 
    min(date) as start_date, 
    max(date) as end_date 
FROM `dnd-trends-index.dnd_trends_raw.trend_data_pilot`
