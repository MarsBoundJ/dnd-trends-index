
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"

def setup_gold():
    client = bigquery.Client()
    
    # 1. Create Dataset if not exists
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = "US"
    try:
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET_ID}")
    except Exception:
        print(f"Dataset {DATASET_ID} details updated/exists.")

    # 2. Trend Score View
    # Formula: Score = (0.4 * Hype) + (0.3 * Play) + (0.3 * Buy)
    # Hype = Avg(Wiki, Fandom)
    # Play = Roll20
    # Buy = 0 (for now, placeholder)
    
    sql_gold = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.trend_scores` AS
    WITH daily_scores AS (
        SELECT 
            COALESCE(w.date, f.date, r.date) as date,
            COALESCE(w.keyword, f.keyword, r.keyword) as keyword,
            w.score_wiki,
            f.score_fandom,
            r.score_roll20
        FROM `{PROJECT_ID}.silver_data.norm_wikipedia` w
        FULL OUTER JOIN `{PROJECT_ID}.silver_data.norm_fandom` f
            ON w.date = f.date AND w.keyword = f.keyword
        FULL OUTER JOIN `{PROJECT_ID}.silver_data.norm_roll20` r
            ON w.date = r.date AND w.keyword = r.keyword
    )
    SELECT
        date,
        keyword,
        COALESCE(score_wiki, 0) as norm_wiki,
        COALESCE(score_fandom, 0) as norm_fandom,
        COALESCE(score_roll20, 0) as norm_roll20,
        
        -- Hype Component (Wiki + Fandom)
        (
            (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0)) / 
            (CASE WHEN score_wiki IS NOT NULL AND score_fandom IS NOT NULL THEN 2 
                  WHEN score_wiki IS NOT NULL OR score_fandom IS NOT NULL THEN 1 
                  ELSE 1 END)
        ) as hype_score,
        
        -- Play Component (Roll20)
        COALESCE(score_roll20, 0) as play_score,
        
        -- Composite Trend Score (Weighted)
        (
            (
             ((COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0)) / 2) * 0.4
            ) + 
            (COALESCE(score_roll20, 0) * 0.3)
            -- Buy component is 0 for now
        ) * 100 as trend_score_raw
        
    FROM daily_scores
    WHERE date IS NOT NULL
    """

    print("Creating View: Trend Scores...")
    try:
        client.query(sql_gold).result()
        print("Success.")
    except Exception as e:
        print(f"Error creating Gold View: {e}")

if __name__ == "__main__":
    setup_gold()
