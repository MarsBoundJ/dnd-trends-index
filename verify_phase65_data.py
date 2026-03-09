from google.cloud import bigquery

client = bigquery.Client()

print("--- Migration Proof (Class) ---")
query1 = """
    SELECT date, category, ruleset_tag, market_share 
    FROM `dnd-trends-index.gold_data.study_edition_migration` 
    WHERE category = 'Class' 
    ORDER BY date DESC, ruleset_tag 
    LIMIT 4
"""
for row in client.query(query1):
    print(row)

print("\n--- Shortage Proof ---")
query2 = """
    SELECT date, player_interest, dm_interest, demand_ratio 
    FROM `dnd-trends-index.gold_data.study_dm_shortage` 
    ORDER BY date DESC 
    LIMIT 2
"""
for row in client.query(query2):
    print(row)

print("\n--- Vista Summary API Data Check ---")
query3 = """
    WITH recent_trends AS (
        SELECT 
            m.canonical_concept as concept_name,
            t.date,
            SUM(t.interest) as interest
        FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
        JOIN `dnd-trends-index.silver_data.view_google_mapping` m ON t.term_id = m.term_id
        WHERE t.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
        GROUP BY 1, 2
    ),
    velocity_calc AS (
        SELECT 
            concept_name,
            SUM(IF(date = (SELECT MAX(date) FROM recent_trends), interest, 0)) as current_val,
            SUM(IF(date = (SELECT MIN(date) FROM recent_trends), interest, 0)) as old_val
        FROM recent_trends
        GROUP BY 1
    )
    SELECT concept_name, (current_val - old_val) as velocity
    FROM velocity_calc
    ORDER BY velocity DESC LIMIT 1
"""
for row in client.query(query3):
    print(f"Rising Tide: {row.concept_name} (Velocity: {row.velocity})")
