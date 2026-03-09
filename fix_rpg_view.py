from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')

rpg_sql = """
WITH latest_stats AS (
    SELECT 
        s.concept_name,
        s.owned_count,
        s.quality_score,
        s.date
    FROM `dnd-trends-index.dnd_trends_raw.rpggeek_product_stats` s
    WHERE s.date = (SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_raw.rpggeek_product_stats`)
),
latest_norm AS (
    SELECT keyword, score_buy, date
    FROM `dnd-trends-index.silver_data.norm_buy`
    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.silver_data.norm_buy`)
)
SELECT 
    COALESCE(c.category, 'Rulebooks') as category,
    l.concept_name,
    l.owned_count,
    l.quality_score,
    COALESCE(n.score_buy, 0.5) * 100 as metric_score,
    'rpggeek' as source,
    RANK() OVER(PARTITION BY COALESCE(c.category, 'Rulebooks') ORDER BY l.owned_count DESC) as rank_position,
    COALESCE(n.score_buy, 0.5) * 100 as heat_score
FROM latest_stats l
LEFT JOIN latest_norm n ON l.concept_name = n.keyword
LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON l.concept_name = c.concept_name
"""

view_id = 'dnd-trends-index.gold_data.view_rpggeek_leaderboards'
view = bigquery.Table(view_id)
view.view_query = rpg_sql

client.delete_table(view_id, not_found_ok=True)
client.create_table(view)
print('Updated RPGGeek view with flexible join.')
