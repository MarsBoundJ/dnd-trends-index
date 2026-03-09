from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index')

sql_norm_yt = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_youtube` AS
WITH daily_raw AS (
    SELECT 
        DATE(processing_date) as date,
        concept_name as keyword,
        AVG(sentiment_score) as max_velocity
    FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence`
    GROUP BY 1, 2
)
SELECT 
    date,
    keyword,
    PERCENT_RANK() OVER(PARTITION BY date ORDER BY max_velocity ASC) as score_youtube
FROM daily_raw
"""

sql_divergence = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_sentiment_divergence` AS
SELECT
    video_id,
    concept_name as title,
    CAST(NULL AS STRING) as channel_name,
    sentiment_score as velocity_24h,
    sentiment_score as sentiment_pos_ratio
FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence`
WHERE sentiment_score < 0.3
"""

sql_trend_scores = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.trend_scores` AS
WITH universe AS (
        SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_wikipedia`
        UNION DISTINCT
        SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_fandom`
        UNION DISTINCT
        SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_roll20`
        UNION DISTINCT
        SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_youtube`
        UNION DISTINCT
        SELECT date, search_term as keyword FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
    ),
    daily_scores AS (
        SELECT 
            u.date,
            u.keyword,
            w.score_wiki,
            f.score_fandom,
            r.score_roll20,
            y.score_youtube,
            g.interest as score_google
        FROM universe u
        LEFT JOIN `dnd-trends-index.silver_data.norm_wikipedia` w
            ON u.date = w.date AND u.keyword = w.keyword
        LEFT JOIN `dnd-trends-index.silver_data.norm_fandom` f
            ON u.date = f.date AND u.keyword = f.keyword
        LEFT JOIN `dnd-trends-index.silver_data.norm_roll20` r
            ON u.date = r.date AND u.keyword = r.keyword
        LEFT JOIN `dnd-trends-index.silver_data.norm_youtube` y
            ON u.date = y.date AND u.keyword = y.keyword
        LEFT JOIN `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` g
            ON u.date = g.date AND u.keyword = g.search_term
    )
    SELECT
        ds.date,
        ds.keyword,
        cat.category,  
        COALESCE(score_wiki, 0) as norm_wiki,
        COALESCE(score_fandom, 0) as norm_fandom,
        COALESCE(score_roll20, 0) as norm_roll20,
        COALESCE(score_youtube, 0) as norm_youtube,
        COALESCE(score_google, 0) as norm_google,
        
        (
            (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0) + COALESCE(score_youtube, 0) + COALESCE(score_google, 0)) / 
            GREATEST(1, 
                IF(score_wiki IS NOT NULL, 1, 0) +
                IF(score_fandom IS NOT NULL, 1, 0) +
                IF(score_youtube IS NOT NULL, 1, 0) +
                IF(score_google IS NOT NULL, 1, 0)
            )
        ) as hype_score,
        
        COALESCE(score_roll20, 0) as play_score,
        
        0.0 as buy_score,

        (
            (
                (
                    (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0) + COALESCE(score_youtube, 0) + COALESCE(score_google, 0)) / 
                    GREATEST(1, 
                        IF(score_wiki IS NOT NULL, 1, 0) +
                        IF(score_fandom IS NOT NULL, 1, 0) +
                        IF(score_youtube IS NOT NULL, 1, 0) +
                        IF(score_google IS NOT NULL, 1, 0)
                    )
                ) * 0.5
            ) + 
            (COALESCE(score_roll20, 0) * 0.3)
        ) * 100 as trend_score_raw
        
    FROM daily_scores ds
    LEFT JOIN (
        SELECT DISTINCT original_keyword, category 
        FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
    ) cat ON ds.keyword = cat.original_keyword
    WHERE ds.date IS NOT NULL
"""

def apply_view(name, q):
    try:
        client.query(q).result()
        print(f"SUCCESS: {name}")
    except Exception as e:
        print(f"ERROR {name}: {e}")

apply_view("norm_youtube", sql_norm_yt)
apply_view("divergence", sql_divergence)
apply_view("trend_scores", sql_trend_scores)
