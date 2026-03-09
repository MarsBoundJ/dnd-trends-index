from google.cloud import bigquery
import os

# Set credentials for container
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

ddl_query = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_crowdfunding` AS
SELECT 
    CAST(project_id AS STRING) AS project_id,
    name AS title,
    creator,
    pledged_usd AS funding_usd,
    backers_count,
    category,
    url,
    discovered_at AS scraped_at,
    'kickstarter' AS source
FROM `dnd-trends-index.commercial_data.kickstarter_projects`
WHERE is_dnd_centric = TRUE

UNION ALL

SELECT 
    project_id,
    title,
    creator,
    funding_usd,
    backers_count,
    system_tag AS category,
    source_url AS url,
    scraped_at,
    'backerkit' AS source
FROM `dnd-trends-index.commercial_data.backerkit_projects`
"""

print("Executing DDL to update silver_data.norm_crowdfunding...")
job = client.query(ddl_query)
job.result()  # Wait for the job to complete
print("View successfully updated/created!")
