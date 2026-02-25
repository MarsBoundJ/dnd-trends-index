-- Silver Layer: Crowdfunding Normalization
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_crowdfunding` AS
WITH raw_data AS (
    SELECT date, project_name as name, daily_pledge_increase as velocity, 'kickstarter' as platform FROM `dnd-trends-index.dnd_trends_raw.kickstarter_daily`
    UNION ALL
    SELECT date, project_name as name, daily_pledge_increase as velocity, 'backerkit' as platform FROM `dnd-trends-index.dnd_trends_raw.backerkit_daily`
)
SELECT 
    date, 
    name, 
    velocity, 
    platform,
    PERCENT_RANK() OVER (PARTITION BY date ORDER BY velocity ASC) as score_crowdfund
FROM raw_data;
