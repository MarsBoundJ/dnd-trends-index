from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
query = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_itchio_predictions` AS 
WITH cluster_counts AS (
  SELECT cluster, DATE(collected_date) as day, COUNT(*) as daily_count 
  FROM `dnd-trends-index.dnd_trends_raw.itchio_products`, 
  UNNEST(aesthetic_clusters) AS cluster 
  GROUP BY 1, 2
) 
SELECT 
  cluster as aesthetic_cluster, 
  day as observation_date, 
  daily_count as new_products, 
  SUM(daily_count) OVER (PARTITION BY cluster ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as velocity_7d 
FROM cluster_counts 
ORDER BY day DESC, velocity_7d DESC;
"""
client.query(query).result()
print('View created successfully.')
