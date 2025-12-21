SELECT 
  count(*) as total_rows,
  count(DISTINCT search_term) as distinct_terms
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
