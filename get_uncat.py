from google.cloud import bigquery
import json
client = bigquery.Client(project='dnd-trends-index')
query = """
SELECT keyword as name, sum(weighted_score) as total_score 
FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics` 
WHERE category IS NULL 
   OR TRIM(category) = '' 
   OR LOWER(category) IN ('uncategorized', 'other', 'unknown')
GROUP BY keyword 
ORDER BY total_score DESC 
LIMIT 50
"""
results = [dict(row) for row in client.query(query)]
with open('top_uncat.json', 'w') as f:
    json.dump(results, f, indent=2)
