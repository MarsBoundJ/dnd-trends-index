
CREATE TABLE `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
(
  term_id STRING OPTIONS(description="Foreign key to expanded_search_terms"),
  search_term STRING OPTIONS(description="The term queried"),
  date DATE OPTIONS(description="The trend data point date"),
  interest INTEGER OPTIONS(description="Google Trends Interest score (0-100)"),
  is_partial BOOL OPTIONS(description="Whether the data point is partial (from pytrends)"),
  fetched_at TIMESTAMP OPTIONS(description="When this data was pulled"),
  batch_id STRING OPTIONS(description="ID of the execution batch")
);
