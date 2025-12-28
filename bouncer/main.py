
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
def get_daily_trends(request):
    # 1. Handle CORS (Allow GitHub Pages to talk to us)
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}

    # 2. Query BigQuery
    client = bigquery.Client()
    
    # Matching the schema from Phase 16 (gold_data.trend_scores)
    query = """
        SELECT 
            keyword as name, 
            trend_score_raw as score, 
            hype_score, 
            play_score,
            CASE 
                WHEN trend_score_raw > 80 THEN 'Legendary'
                WHEN trend_score_raw > 60 THEN 'Very Rare'
                WHEN trend_score_raw > 40 THEN 'Rare'
                ELSE 'Common'
            END as rarity
        FROM `dnd-trends-index.gold_data.trend_scores`
        WHERE date = CURRENT_DATE()
        ORDER BY score DESC
        LIMIT 50
    """
    
    try:
        query_job = client.query(query)
        # BigQuery rows to dict
        results = [dict(row) for row in query_job]
        
        # 3. Return JSON
        return (json.dumps(results, default=json_serial), 200, headers)
        
    except Exception as e:
        error_msg = {"error": str(e)}
        return (json.dumps(error_msg), 500, headers)
