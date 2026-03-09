from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Task 1: Market Equilibrium View
# Correct columns: social uses 'name'/'metric_score', api uses 'canonical_concept'/'google_score_avg'
equilibrium_view_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.study_market_equilibrium` AS
WITH social_data AS (
    SELECT 
        name as concept_name,
        AVG(metric_score) as social_score
    FROM `dnd-trends-index.gold_data.view_social_leaderboards`
    GROUP BY 1
),
sales_data AS (
    SELECT 
        canonical_concept as concept_name,
        AVG(google_score_avg) as trend_score
    FROM `dnd-trends-index.gold_data.view_api_leaderboards`
    GROUP BY 1
),
combined AS (
    SELECT 
        s.concept_name,
        s.social_score,
        t.trend_score,
        SAFE_DIVIDE(s.social_score, t.trend_score) as hype_ratio
    FROM social_data s
    JOIN sales_data t ON s.concept_name = t.concept_name
)
SELECT 
    concept_name,
    social_score,
    trend_score,
    hype_ratio,
    CASE 
        WHEN social_score > 60 AND trend_score < 30 THEN 'Hype Bubble'
        WHEN social_score < 30 AND trend_score > 60 THEN 'Quiet Giant'
        WHEN social_score > 60 AND trend_score > 60 THEN 'Market Leader'
        ELSE 'Neutral'
    END as classification
FROM combined
"""

print("Creating Market Equilibrium View v2...")
try:
    client.query(equilibrium_view_sql).result()
    print("Market Equilibrium View created successfully.")
except Exception as e:
    print(f"Error creating Market Equilibrium View: {e}")

# Equilibrium Report Checkpoint
print("\n--- Equilibrium Report ---")
try:
    # #1 Hype Bubble
    bubble = list(client.query("SELECT concept_name, social_score, trend_score FROM `dnd-trends-index.gold_data.study_market_equilibrium` WHERE classification = 'Hype Bubble' ORDER BY hype_ratio DESC LIMIT 1").result())
    if bubble:
        print(f"#1 Hype Bubble: {bubble[0].concept_name} (Social: {bubble[0].social_score:.1f}, Trend: {bubble[0].trend_score:.1f})")
    else:
        print("#1 Hype Bubble: None found (No concepts meet 'High Social / Low Trend' threshold)")

    # #1 Quiet Giant
    giant = list(client.query("SELECT concept_name, social_score, trend_score FROM `dnd-trends-index.gold_data.study_market_equilibrium` WHERE classification = 'Quiet Giant' ORDER BY hype_ratio ASC LIMIT 1").result())
    if giant:
        print(f"#1 Quiet Giant: {giant[0].concept_name} (Social: {giant[0].social_score:.1f}, Trend: {giant[0].trend_score:.1f})")
    else:
        print("#1 Quiet Giant: None found (No concepts meet 'Low Social / High Trend' threshold)")

except Exception as e:
    print(f"Error running report: {e}")
