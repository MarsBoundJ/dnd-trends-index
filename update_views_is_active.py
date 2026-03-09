from google.cloud import bigquery

client = bigquery.Client()

def update_social_view():
    print("Updating gold_data.view_social_leaderboards...")
    sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_social_leaderboards` AS
WITH daily_agg AS (
    SELECT 
        keyword as name,
        extraction_date as date,
        AVG(weighted_score) as sentiment, 
        SUM(mention_count) as mentions
    FROM `dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics`
    GROUP BY 1, 2
),
history_trails AS (
    SELECT 
        name,
        ARRAY_AGG(
            CASE 
                WHEN mentions > 10 THEN 1 -- High volume spike
                WHEN mentions < 2 THEN -1 -- Low volume drop
                ELSE 0
            END ORDER BY date DESC LIMIT 7
        ) as history
    FROM daily_agg
    GROUP BY 1
),
latest_stats AS (
    SELECT 
        name,
        sentiment,
        mentions as score
    FROM daily_agg
    WHERE date = (SELECT MAX(date) FROM daily_agg)
),
joined_data AS (
    SELECT 
        COALESCE(c.category, 'Uncategorized') as category,
        l.name,
        l.score as metric_score,
        l.sentiment,
        COALESCE(h.history, []) as history,
        'reddit' as source
    FROM latest_stats l
    INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON l.name = c.concept_name
    LEFT JOIN history_trails h ON l.name = h.name
    WHERE c.is_active = TRUE
),
sector_stats AS (
    SELECT 
        category, 
        AVG(metric_score) as heat_score
    FROM joined_data
    GROUP BY 1
)
SELECT 
    j.category,
    j.name,
    j.metric_score,
    j.sentiment,
    j.history,
    j.source,
    RANK() OVER(PARTITION BY j.category ORDER BY j.metric_score DESC) as rank_position,
    s.heat_score
FROM joined_data j
JOIN sector_stats s ON j.category = s.category
    """
    client.query(sql).result()
    print("Social view updated.")

def update_api_view():
    print("Updating gold_data.view_api_leaderboards...")
    sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_api_leaderboards` AS
WITH date_stats AS (
    SELECT 
        category, 
        date, 
        COUNT(*) as item_count
    FROM `dnd-trends-index.gold_data.deep_dive_metrics`
    GROUP BY 1, 2
),
max_category_volume AS (
    SELECT category, MAX(item_count) as max_vol
    FROM date_stats
    GROUP BY 1
),
stable_latest_dates AS (
    SELECT 
        ds.category, 
        MAX(ds.date) as latest_stable_date
    FROM date_stats ds
    JOIN max_category_volume mv ON ds.category = mv.category
    WHERE ds.item_count >= mv.max_vol * 0.8
    GROUP BY 1
),
ranked AS (
    SELECT 
        m.category,
        m.canonical_concept,
        m.google_score_avg,
        ROW_NUMBER() OVER (PARTITION BY m.category ORDER BY m.google_score_avg DESC) as rank_position
    FROM `dnd-trends-index.gold_data.deep_dive_metrics` m
    INNER JOIN stable_latest_dates sld ON m.category = sld.category AND m.date = sld.latest_stable_date
    INNER JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON m.canonical_concept = c.concept_name
    WHERE c.is_active = TRUE
),
sector_stats AS (
    SELECT 
        category, 
        AVG(google_score_avg) as heat_score
    FROM ranked 
    WHERE rank_position <= 20
    GROUP BY 1
)
SELECT 
    r.category,
    r.canonical_concept,
    r.google_score_avg,
    r.rank_position,
    s.heat_score
FROM ranked r
JOIN sector_stats s ON r.category = s.category
WHERE r.rank_position <= 40
ORDER BY s.heat_score DESC, r.rank_position ASC
    """
    client.query(sql).result()
    print("API view updated.")

if __name__ == "__main__":
    update_social_view()
    update_api_view()
