from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

sql_crowdfunding = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_crowdfunding` AS
WITH combined AS (
    SELECT 
        date,
        project_name as name,
        funding_velocity as velocity,
        'kickstarter' as platform
    FROM `dnd-trends-index.dnd_trends_raw.kickstarter_daily`
    UNION ALL
    SELECT 
        date,
        project_name as name,
        funding_velocity as velocity,
        'backerkit' as platform
    FROM `dnd-trends-index.dnd_trends_raw.backerkit_daily`
)
SELECT 
    date,
    name,
    platform,
    velocity,
    PERCENT_RANK() OVER(PARTITION BY date ORDER BY velocity ASC) as score_crowdfund
FROM combined
"""

print("Updating norm_crowdfunding...")
client.query(sql_crowdfunding).result()
print("Success.")
