from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Task 2: Subclass Graveyard View
# Advanced regex mapping for parent classes from tags
graveyard_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_subclass_graveyard` AS
WITH subclass_base AS (
    SELECT 
        lib.concept_name as subclass_name,
        tag,
        REGEXP_EXTRACT(tag, r'^(.*)_Subclasses$') as class_from_tag,
        REGEXP_EXTRACT(tag, r'^Subclass:(.*)$') as class_from_alt_tag
    FROM `dnd-trends-index.dnd_trends_categorized.concept_library` lib,
    UNNEST(tags) as tag
    WHERE LOWER(category) = 'subclass'
),
mapped_subclasses AS (
    SELECT 
        subclass_name,
        COALESCE(class_from_tag, class_from_alt_tag) as class_name
    FROM subclass_base
    WHERE COALESCE(class_from_tag, class_from_alt_tag) IS NOT NULL
),
subclass_interest AS (
    SELECT 
        t.date,
        s.subclass_name,
        s.class_name,
        t.interest
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    JOIN `dnd-trends-index.silver_data.view_google_mapping` m ON t.term_id = m.term_id
    JOIN mapped_subclasses s ON m.canonical_concept = s.subclass_name
),
class_totals AS (
    SELECT 
        class_name,
        SUM(interest) as total_class_volume
    FROM subclass_interest
    GROUP BY 1
),
subclass_totals AS (
    SELECT 
        class_name,
        subclass_name,
        SUM(interest) as subclass_volume
    FROM subclass_interest
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

print("Creating Subclass Graveyard View with Regex mapping...")
try:
    client.query(graveyard_view_sql).result()
    print("Subclass Graveyard View created successfully.")
except Exception as e:
    print(f"Error creating Graveyard View: {e}")

# Checkpoint Report: Top 3 subclasses in Graveyard (< 2% share)
print("\n--- Graveyard Report (Top 3) ---")
report_sql = "SELECT class_name, subclass_name, subclass_share FROM `dnd-trends-index.gold_data.study_subclass_graveyard` WHERE subclass_share < 0.02 LIMIT 3"
try:
    results = list(client.query(report_sql).result())
    if not results:
        print("No subclasses found with < 2% share. Listing top 3 absolute smallest:")
        report_sql = "SELECT class_name, subclass_name, subclass_share FROM `dnd-trends-index.gold_data.study_subclass_graveyard` ORDER BY subclass_share ASC LIMIT 3"
        results = list(client.query(report_sql).result())
    
    for row in results:
        print(f"[{row.class_name}] {row.subclass_name}: {row.subclass_share*100:.2f}% share")
except Exception as e:
    print(f"Error running report: {e}")
