from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

sql_crowdfunding = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_crowdfunding` AS
WITH combined AS (
    SELECT 
        DATE(discovered_at) as date,
        name,
        pledged_usd as velocity,
        'kickstarter' as platform
    FROM `dnd-trends-index.commercial_data.kickstarter_projects`
    WHERE pledged_usd IS NOT NULL
    UNION ALL
    SELECT 
        DATE(scraped_at) as date,
        title as name,
        funding_usd as velocity,
        'backerkit' as platform
    FROM `dnd-trends-index.commercial_data.backerkit_projects`
    WHERE funding_usd IS NOT NULL
)
SELECT 
    date,
    name,
    platform,
    velocity,
    PERCENT_RANK() OVER(PARTITION BY date ORDER BY velocity ASC) as score_crowdfund
FROM combined
"""

print("Updating norm_crowdfunding with correct commercial_data tables...")
client.query(sql_crowdfunding).result()
print("Success. View created.")
