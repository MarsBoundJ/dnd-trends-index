-- Crowdfunding Analytics: The Wallet Vote
-- Dataset: gold_data.analytics_crowdfunding
-- Per-campaign funding analysis with archetypes, velocity, and category-level demand signals
-- Combines Kickstarter and BackerKit data
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_crowdfunding`
--        WHERE campaign_archetype = 'overfunded_hit' AND is_dnd = TRUE

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_crowdfunding` AS

WITH -- Kickstarter projects (latest snapshot per project)
ks_latest AS (
  SELECT project_id, name, creator, backers_count, pledged_usd, goal_usd,
    percent_funded, category, status, is_dnd_centric, end_date, discovered_at,
    ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY discovered_at DESC) as rn
  FROM `dnd-trends-index.commercial_data.kickstarter_projects`
),
kickstarter AS (
  SELECT
    CAST(project_id AS STRING) as project_id,
    name as project_name,
    creator,
    backers_count,
    pledged_usd,
    goal_usd,
    percent_funded as overfund_pct,
    category,
    status,
    is_dnd_centric as is_dnd,
    'kickstarter' as platform,
    discovered_at
  FROM ks_latest WHERE rn = 1
),

-- BackerKit projects (latest snapshot per project)
bk_latest AS (
  SELECT project_id, title, creator, funding_usd, backers_count,
    days_remaining, system_tag, scraped_at,
    ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY scraped_at DESC) as rn
  FROM `dnd-trends-index.commercial_data.backerkit_projects`
),
backerkit AS (
  SELECT
    project_id,
    title as project_name,
    creator,
    backers_count,
    funding_usd as pledged_usd,
    CAST(NULL AS FLOAT64) as goal_usd,
    CAST(NULL AS FLOAT64) as overfund_pct,
    system_tag as category,
    CASE WHEN days_remaining > 0 THEN 'live' ELSE 'ended' END as status,
    CASE WHEN system_tag LIKE '%5e%' OR system_tag LIKE '%D&D%' THEN TRUE ELSE FALSE END as is_dnd,
    'backerkit' as platform,
    scraped_at as discovered_at
  FROM bk_latest WHERE rn = 1
),

-- Union both platforms
all_campaigns AS (
  SELECT * FROM kickstarter
  UNION ALL
  SELECT * FROM backerkit
)

SELECT
  c.project_id,
  c.project_name,
  c.creator,
  c.platform,
  c.category,
  c.status,
  c.is_dnd,

  -- Funding metrics
  ROUND(c.pledged_usd, 2) as pledged_usd,
  ROUND(COALESCE(c.goal_usd, 0), 2) as goal_usd,
  c.backers_count,

  -- Overfund ratio (Kickstarter only, BackerKit lacks goal)
  ROUND(COALESCE(c.overfund_pct, 0), 1) as overfund_pct,
  ROUND(SAFE_DIVIDE(c.pledged_usd, GREATEST(COALESCE(c.goal_usd, 0), 1)), 2) as overfund_ratio,

  -- Average pledge per backer
  ROUND(SAFE_DIVIDE(c.pledged_usd, GREATEST(c.backers_count, 1)), 2) as avg_pledge,

  -- Campaign archetype classification
  CASE
    -- Overfunded hit: funded >300% of goal
    WHEN COALESCE(c.overfund_pct, 0) >= 300
      THEN 'overfunded_hit'
    -- Day-one spike: funded quickly (>100% funded, still live or recently ended)
    WHEN COALESCE(c.overfund_pct, 0) >= 100 AND c.backers_count >= 500
      THEN 'strong_performer'
    -- Funded but modest
    WHEN COALESCE(c.overfund_pct, 0) >= 100 AND COALESCE(c.overfund_pct, 0) < 300
      THEN 'funded_modest'
    -- Still in progress (live, partial funding)
    WHEN c.status = 'live' AND COALESCE(c.overfund_pct, 0) < 100
      THEN 'in_progress'
    -- Failed: ended without reaching goal
    WHEN c.status != 'live' AND COALESCE(c.overfund_pct, 0) < 100
      THEN 'failed'
    -- BackerKit campaigns without goal data: classify by funding volume
    WHEN c.goal_usd IS NULL AND c.pledged_usd >= 100000
      THEN 'high_volume'
    WHEN c.goal_usd IS NULL
      THEN 'backerkit_active'
    ELSE 'other'
  END as campaign_archetype,

  -- Backer tier classification
  CASE
    WHEN c.backers_count >= 5000 THEN 'mega'
    WHEN c.backers_count >= 1000 THEN 'large'
    WHEN c.backers_count >= 200 THEN 'medium'
    ELSE 'small'
  END as backer_tier,

  -- Pledge tier (avg pledge indicates audience type)
  CASE
    WHEN SAFE_DIVIDE(c.pledged_usd, GREATEST(c.backers_count, 1)) >= 100 THEN 'premium_collector'
    WHEN SAFE_DIVIDE(c.pledged_usd, GREATEST(c.backers_count, 1)) >= 40 THEN 'standard_supporter'
    WHEN SAFE_DIVIDE(c.pledged_usd, GREATEST(c.backers_count, 1)) >= 15 THEN 'casual_backer'
    ELSE 'low_tier'
  END as pledge_tier,

  -- DnD filter for "missing feature heatmap" analysis
  -- When is_dnd = TRUE and overfunded, this represents unmet demand WotC isn't serving
  CASE
    WHEN c.is_dnd = TRUE AND COALESCE(c.overfund_pct, 0) >= 200
      THEN TRUE
    ELSE FALSE
  END as is_unmet_demand_signal,

  -- Rank by total pledged
  ROW_NUMBER() OVER (ORDER BY c.pledged_usd DESC) as rank_by_funding,

  -- Rank within D&D-centric only
  CASE WHEN c.is_dnd = TRUE THEN
    ROW_NUMBER() OVER (
      PARTITION BY c.is_dnd
      ORDER BY c.pledged_usd DESC
    )
  ELSE NULL END as rank_dnd_only,

  -- Standardized output contract
  'financial' as signal_type,
  ROUND(c.pledged_usd, 2) as primary_metric,
  -- Momentum: overfund ratio as proxy (>1 = gaining, <1 = struggling)
  ROUND(SAFE_DIVIDE(c.pledged_usd, GREATEST(COALESCE(c.goal_usd, c.pledged_usd), 1)) - 1.0, 3) as momentum,
  CASE
    WHEN c.backers_count >= 100 AND c.pledged_usd >= 10000 THEN 'HIGH'
    WHEN c.backers_count >= 20 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'crowdfunding' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM all_campaigns c;
