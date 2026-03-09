CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_crowdfunding` AS
WITH raw_data AS (
    -- RECONSTRUCTION: Using commercial_data projects because dnd_trends_raw.kickstarter_daily IS MISSING
    SELECT 
        CAST(discovered_at AS DATE) as date, 
        name, 
        pledged_usd as velocity,
        'kickstarter' as platform 
    FROM `dnd-trends-index.commercial_data.kickstarter_projects`
    UNION ALL
    SELECT 
        CAST(scraped_at AS DATE) as date, 
        title as name, 
        funding_usd as velocity,
        'backerkit' as platform 
    FROM `dnd-trends-index.commercial_data.backerkit_projects`
)
SELECT 
    date, 
    name, 
    velocity, 
    platform,
    PERCENT_RANK() OVER (PARTITION BY date ORDER BY velocity ASC) as score_crowdfund
FROM raw_data;
