ß
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "silver_data"

def setup_silver():
    client = bigquery.Client()
    
    # 1. Create Dataset if not exists
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = "US"
    try:
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET_ID}")
    except Exception:
        print(f"Dataset {DATASET_ID} details updated/exists.")

    # 2. Normalize Wikipedia Views (Higher Views = Better)
    # View: silver_data.norm_wikipedia
    sql_wiki = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.norm_wikipedia` AS
    SELECT
        date,
        article_title as keyword,
        views,
        PERCENT_RANK() OVER (PARTITION BY date ORDER BY views ASC) as score_wiki
    FROM `{PROJECT_ID}.social_data.wikipedia_daily_views`
    """
    
    # 3. Normalize Fandom Ranks (Lower Rank = Better)
    # View: silver_data.norm_fandom
    # We invert rank: Rank 1 (Top) should be score ~1.0. Rank 100 should be ~0.0.
    # PERCENT_RANK with DESC orders largest value first. 
    # But Rank 100 > Rank 1. So ORDER BY rank DESC gives Rank 100 a percentile of 0? No.
    # Rank 100 (largest number) would be first in DESC sort? No, DESC starts with largest.
    # Row 1: 100 (Percent Rank 0.0)
    # Row N: 1 (Percent Rank 1.0)
    # So ORDER BY rank DESC is correct for "Low Rank Number is Good".
    sql_fandom = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.norm_fandom` AS
    SELECT
        snapshot_date as date,
        article_title as keyword,
        wiki_name,
        rank,
        PERCENT_RANK() OVER (PARTITION BY snapshot_date, wiki_name ORDER BY rank DESC) as score_fandom
    FROM `{PROJECT_ID}.social_data.fandom_trending`
    """

    # 4. Normalize Roll20 Ranks (Lower Rank = Better, same logic)
    # View: silver_data.norm_roll20
    sql_roll20 = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.norm_roll20` AS
    SELECT
        snapshot_date as date,
        title as keyword,
        rank,
        PERCENT_RANK() OVER (PARTITION BY snapshot_date ORDER BY rank DESC) as score_roll20
    FROM `{PROJECT_ID}.commercial_data.roll20_rankings`
    """

    # Execute
    for name, sql in [("Wiki", sql_wiki), ("Fandom", sql_fandom), ("Roll20", sql_roll20)]:
        print(f"Creating View: {name}...")
        try:
            client.query(sql).result()
            print("Success.")
        except Exception as e:
            print(f"Error creating {name}: {e}")

if __name__ == "__main__":
    setup_silver()
ß"(0f1ceee2742f32be6a66898aa01f4fd3b072102f24file:///C:/Users/Yorri/.gemini/setup_silver_layer.py:file:///C:/Users/Yorri/.gemini