
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
            COALESCE(w.date, f.date, r.date, y.date) as date,
            COALESCE(w.keyword, f.keyword, r.keyword, y.keyword) as keyword,
            w.score_wiki,
            f.score_fandom,
            r.score_roll20,
            y.score_youtube
        FROM `{PROJECT_ID}.silver_data.norm_wikipedia` w
        FULL OUTER JOIN `{PROJECT_ID}.silver_data.norm_fandom` f
            ON w.date = f.date AND w.keyword = f.keyword
        FULL OUTER JOIN `{PROJECT_ID}.silver_data.norm_roll20` r
            ON w.date = r.date AND w.keyword = r.keyword
        FULL OUTER JOIN `{PROJECT_ID}.silver_data.norm_youtube` y
            ON (w.date = y.date OR f.date = y.date OR r.date = y.date) AND (w.keyword = y.keyword OR f.keyword = y.keyword OR r.keyword = y.keyword)
    )
    SELECT
        date,
        keyword,
        COALESCE(score_wiki, 0) as norm_wiki,
        COALESCE(score_fandom, 0) as norm_fandom,
        COALESCE(score_roll20, 0) as norm_roll20,
        COALESCE(score_youtube, 0) as norm_youtube,
        
        -- Hype Component (Wiki + Fandom + YouTube)
        (
            (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0) + COALESCE(score_youtube, 0)) / 
            (CASE 
                WHEN score_wiki IS NOT NULL AND score_fandom IS NOT NULL AND score_youtube IS NOT NULL THEN 3
                WHEN (score_wiki IS NOT NULL AND score_fandom IS NOT NULL) OR (score_wiki IS NOT NULL AND score_youtube IS NOT NULL) OR (score_fandom IS NOT NULL AND score_youtube IS NOT NULL) THEN 2
                ELSE 1 END)
        ) as hype_score,
        
        -- Play Component (Roll20)
        COALESCE(score_roll20, 0) as play_score,
        
        -- Buy Component (Placeholder 0)
        0.0 as buy_score,

        -- Composite Trend Score (Weighted)
        -- Hype (40%) + Play (30%) + Buy (30%)
        (
            ((
                (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0) + COALESCE(score_youtube, 0)) / 
                (CASE 
                    WHEN score_wiki IS NOT NULL AND score_fandom IS NOT NULL AND score_youtube IS NOT NULL THEN 3
                    WHEN (score_wiki IS NOT NULL AND score_fandom IS NOT NULL) OR (score_wiki IS NOT NULL AND score_youtube IS NOT NULL) OR (score_fandom IS NOT NULL AND score_youtube IS NOT NULL) THEN 2
                    ELSE 1 END)
            ) * 0.4) + 
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
