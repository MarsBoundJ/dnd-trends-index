from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Task 0: Fix Mapping View
mapping_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.view_google_mapping` AS
SELECT 
    m.term_id,
    m.search_term as google_keyword,
    m.original_keyword as raw_search_term,
    l.concept_name as canonical_concept,
    l.category,
    l.persona_target
FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` m
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` l ON m.original_keyword = l.concept_name
"""

# Task 1: Edition Migration View
migration_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_edition_migration` AS
WITH raw_trends AS (
    SELECT 
        t.date,
        m.category,
        m.canonical_concept,
        t.interest,
        CASE 
            WHEN m.canonical_concept LIKE '%(2024)%' THEN '2024_REVISION'
            ELSE 'LEGACY_5E'
        END as ruleset_tag
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    JOIN `dnd-trends-index.silver_data.view_google_mapping` m USING (term_id)
),
daily_category_totals AS (
    SELECT 
        date,
        category,
        SUM(interest) as total_category_interest
    FROM raw_trends
    GROUP BY 1, 2
),
ruleset_daily_stats AS (
    SELECT 
        date,
        category,
        ruleset_tag,
        SUM(interest) as ruleset_interest
    FROM raw_trends
    GROUP BY 1, 2, 3
)
SELECT 
    s.date,
    s.category,
    s.ruleset_tag,
    s.ruleset_interest,
    t.total_category_interest,
    SAFE_DIVIDE(s.ruleset_interest, t.total_category_interest) as market_share
FROM ruleset_daily_stats s
JOIN daily_category_totals t ON s.date = t.date AND s.category = t.category
"""

# Task 2: DM Shortage View
shortage_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_dm_shortage` AS
WITH persona_interest AS (
    SELECT 
        t.date,
        m.persona_target,
        SUM(t.interest) as total_interest
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    JOIN `dnd-trends-index.silver_data.view_google_mapping` m USING (term_id)
    WHERE m.persona_target IN ('DM', 'PLAYER')
    GROUP BY 1, 2
),
pivoted AS (
    SELECT 
        date,
        SUM(IF(persona_target = 'PLAYER', total_interest, 0)) as player_interest,
        SUM(IF(persona_target = 'DM', total_interest, 0)) as dm_interest
    FROM persona_interest
    GROUP BY 1
)
SELECT 
    date,
    player_interest,
    dm_interest,
    SAFE_DIVIDE(player_interest, dm_interest) as demand_ratio
FROM pivoted
"""

print("Fixing Mapping View...")
client.query(mapping_view_sql).result()
print("Creating Edition Migration View...")
client.query(migration_view_sql).result()
print("Creating DM Shortage View...")
client.query(shortage_view_sql).result()
print("Views created successfully.")
