from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Task 1: Decay Signature View
decay_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_decay_signatures` AS
WITH post_launch_trends AS (
    SELECT 
        m.event_name,
        m.event_date,
        lib.concept_name as canonical_concept,
        t.date,
        t.interest,
        DATE_DIFF(t.date, m.event_date, DAY) as days_since_launch
    FROM `dnd-trends-index.gold_data.event_milestones` m
    CROSS JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` lib
    JOIN `dnd-trends-index.silver_data.view_google_mapping` map ON lib.concept_name = map.canonical_concept
    JOIN `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t ON map.term_id = t.term_id
    WHERE t.date BETWEEN m.event_date AND DATE_ADD(m.event_date, INTERVAL 30 DAY)
),
peak_interest AS (
    SELECT 
        event_name,
        canonical_concept,
        MAX(interest) as peak_val
    FROM post_launch_trends
    GROUP BY 1, 2
),
half_life_calc AS (
    SELECT 
        p.event_name,
        p.canonical_concept,
        p.days_since_launch,
        p.interest,
        pk.peak_val
    FROM post_launch_trends p
    JOIN peak_interest pk ON p.event_name = pk.event_name AND p.canonical_concept = pk.canonical_concept
    WHERE p.interest <= pk.peak_val * 0.5
)
SELECT 
    event_name,
    canonical_concept,
    MIN(days_since_launch) as half_life_days
FROM half_life_calc
GROUP BY 1, 2
ORDER BY half_life_days ASC
"""

# Task 2: Subclass Graveyard View
graveyard_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_subclass_graveyard` AS
WITH subclass_data AS (
    SELECT 
        t.date,
        lib.concept_name as subclass_name,
        lib.parent_concept as class_name,
        t.interest
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
    JOIN `dnd-trends-index.silver_data.view_google_mapping` m USING (term_id)
    JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` lib ON m.canonical_concept = lib.concept_name
    WHERE lib.category = 'CLASS' AND lib.parent_concept IS NOT NULL AND lib.parent_concept != ''
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

print("Creating Decay Signature View...")
client.query(decay_view_sql).result()
print("Creating Subclass Graveyard View...")
client.query(graveyard_view_sql).result()
print("Views created successfully.")
