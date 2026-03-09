from google.cloud import bigquery

client = bigquery.Client()

print("--- study_edition_migration ---")
try:
    q1 = "SELECT * FROM `dnd-trends-index.gold_data.study_edition_migration` LIMIT 5"
    results = list(client.query(q1).result())
    for row in results:
        print(row)
except Exception as e:
    print(f"Error Migration: {e}")

print("\n--- study_dm_shortage ---")
try:
    q2 = "SELECT * FROM `dnd-trends-index.gold_data.study_dm_shortage` LIMIT 5"
    results = list(client.query(q2).result())
    for row in results:
        print(row)
except Exception as e:
    print(f"Error Shortage: {e}")

print("\n--- Rising Tide Query Test ---")
rising_tide_query = """
WITH recent_trends AS (
    SELECT 
        m.canonical_concept as concept_name,
        t.date as trend_date,
        SUM(t.interest) as interest
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` AS t
    JOIN `dnd-trends-index.silver_data.view_google_mapping` AS m ON t.term_id = m.term_id
    WHERE t.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
    GROUP BY 1, 2
),
velocity_calc AS (
    SELECT 
        concept_name,
        SUM(IF(trend_date = (SELECT MAX(trend_date) FROM recent_trends), interest, 0)) as current_val,
        SUM(IF(trend_date = (SELECT MIN(trend_date) FROM recent_trends), interest, 0)) as old_val
    FROM recent_trends
    GROUP BY 1
)
SELECT concept_name, (CAST(current_val AS FLOAT64) - CAST(old_val AS FLOAT64)) as velocity
FROM velocity_calc
ORDER BY velocity DESC LIMIT 1
"""
try:
    results = list(client.query(rising_tide_query).result())
    for row in results:
        print(row)
except Exception as e:
    print(f"Error Rising Tide: {e}")
