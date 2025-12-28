from google.cloud import bigquery

client = bigquery.Client()
query = """
    SELECT concept_name, ks_mentions, total_pledged_exposure 
    FROM `dnd-trends-index.commercial_data.keyword_commercial_metrics` 
    ORDER BY total_pledged_exposure DESC 
    LIMIT 5
"""
try:
    job = client.query(query)
    print("Top 5 Boosted Keywords:")
    for row in job:
        print(f"- {row.concept_name}: ${row.total_pledged_exposure:,.2f} ({row.ks_mentions} mentions)")
except Exception as e:
    print(f"Error: {e}")
