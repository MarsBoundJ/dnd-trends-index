from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Task 2: Subclass Graveyard View
# Switching USING to ON for robustness.
graveyard_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_subclass_graveyard` AS
WITH subclass_data AS (
    SELECT 
        t.date,
        lib.concept_name as subclass_name,
        lib.parent as class_name,
        t.interest
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    JOIN `dnd-trends-index.silver_data.view_google_mapping` m ON t.term_id = m.term_id
    JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` lib ON m.canonical_concept = lib.concept_name
    WHERE LOWER(lib.category) = 'subclass' AND lib.parent IS NOT NULL AND lib.parent != ''
),
class_totals AS (
    SELECT 
        class_name,
        SUM(interest) as total_class_volume
    FROM subclass_data
    GROUP BY 1
),
subclass_totals AS (
    SELECT 
        class_name,
        subclass_name,
        SUM(interest) as subclass_volume
    FROM subclass_data
    GROUP BY 1, 2
)
SELECT 
    s.class_name,
    s.subclass_name,
    s.subclass_volume,
    c.total_class_volume,
    SAFE_DIVIDE(s.subclass_volume, c.total_class_volume) as subclass_share
FROM subclass_totals s
JOIN class_totals c ON s.class_name = c.class_name
ORDER BY subclass_share ASC
"""

print("Creating Subclass Graveyard View...")
try:
    client.query(graveyard_view_sql).result()
    print("Subclass Graveyard View created successfully.")
except Exception as e:
    print(f"Error creating Graveyard View: {e}")
    import traceback
    traceback.print_exc()
