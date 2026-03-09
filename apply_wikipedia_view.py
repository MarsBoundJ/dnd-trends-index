from google.cloud import bigquery

client = bigquery.Client()

sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_wikipedia_leaderboards` AS
WITH latest_data AS (
    SELECT 
        keyword as canonical_concept,
        views,
        COALESCE(score_wiki * 100, 0) as score
    FROM `dnd-trends-index.silver_data.norm_wikipedia`
    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.silver_data.norm_wikipedia`)
),
categorized AS (
    SELECT 
        l.canonical_concept,
        l.score,
        c.category
    FROM latest_data l
    INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c
        ON LOWER(l.canonical_concept) = LOWER(c.concept_name)
    WHERE c.is_active = TRUE
),
sector_stats AS (
    SELECT category, AVG(score) as heat_score
    FROM categorized
    GROUP BY 1
)
SELECT 
    c.category,
    c.canonical_concept,
    c.score,
    s.heat_score,
    'wikipedia' as source,
    RANK() OVER (PARTITION BY c.category ORDER BY c.score DESC) as rank_position
FROM categorized c
JOIN sector_stats s ON c.category = s.category
ORDER BY s.heat_score DESC, c.score DESC;
"""

print("Applying Wikipedia View...")
query_job = client.query(sql)
query_job.result()
print("Success! View 'gold_data.view_wikipedia_leaderboards' created/updated.")
