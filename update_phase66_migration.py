from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

master_list = ['Weapon Mastery', 'Epic Boon', 'Tactical Mind', 'Fount of Moonlight', 'Starry Wisp', 'Elementalism', 'College of the Moon', 'Circle of the Sea', 'Path of the World Tree']

update_query = f"""
UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
SET ruleset = '2024_REVISION'
WHERE concept_name IN UNNEST({master_list})
"""

try:
    print("Executing Concept Library Update...")
    client.query(update_query).result()
    print("Update successful.")
except Exception as e:
    print(f"Error updating concepts: {e}")

migration_view_sql = f"""
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_edition_migration` AS
WITH raw_trends AS (
    SELECT 
        t.date,
        m.category,
        m.canonical_concept,
        t.interest,
        CASE 
            WHEN m.raw_search_term LIKE '%2024%' OR m.raw_search_term LIKE '%5.5%' OR m.canonical_concept IN UNNEST({master_list}) THEN '2024_REVISION'
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

try:
    print("Re-creating Edition Migration View...")
    client.query(migration_view_sql).result()
    print("View recreation successful.")
except Exception as e:
    print(f"Error recreating view: {e}")

# Verify Migration Index is > 0.0
verify_query = """
SELECT market_share 
FROM `dnd-trends-index.gold_data.study_edition_migration` 
WHERE ruleset_tag = '2024_REVISION' 
ORDER BY date DESC LIMIT 1
"""
try:
    res = list(client.query(verify_query).result())
    if res:
        print(f"New Migration Index (Market Share for 2024_REVISION): {res[0].market_share}")
    else:
        print("No results returned for 2024_REVISION.")
except Exception as e:
    print(f"Error verifying index: {e}")
