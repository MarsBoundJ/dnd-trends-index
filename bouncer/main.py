
import functions_framework
from google.cloud import bigquery
import json
import datetime

# Helper to handle date serialization
def json_serial(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

@functions_framework.http
def bouncer_api(request):
    # 1. Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}
    client = bigquery.Client()
    
    # Simple Router
    path = request.path.strip('/')
    
    if path == 'trends':
        query = """
            SELECT 
                keyword as name, 
                category,
                trend_score_raw as score, 
                hype_score, 
                play_score,
                norm_wiki,
                norm_fandom,
                norm_youtube,
                norm_roll20,
                CASE 
                    WHEN trend_score_raw > 80 THEN 'Legendary'
                    WHEN trend_score_raw > 60 THEN 'Very Rare'
                    WHEN trend_score_raw > 40 THEN 'Rare'
                    ELSE 'Common'
                END as rarity
            FROM `dnd-trends-index.gold_data.trend_scores`
            WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.gold_data.trend_scores`)
            ORDER BY score DESC
            LIMIT 20
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, default=json_serial), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'categories':
        query = """
            SELECT category, trend_count, avg_category_score
            FROM `dnd-trends-index.gold_data.category_leaders`
            ORDER BY avg_category_score DESC
            LIMIT 10
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, default=json_serial), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'news':
        query = """
            SELECT date, headline, hook, body_markdown, key_stat, persona
            FROM `dnd-trends-index.gold_data.daily_articles`
            ORDER BY date DESC, headline ASC
            LIMIT 6
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, default=json_serial), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)
            
    return (json.dumps({"error": "Endpoint not found"}), 404, headers)
