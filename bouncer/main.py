
import functions_framework
from google.cloud import bigquery
import json
import datetime

from decimal import Decimal
from datetime import date, datetime

class ArcaneEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, (date, datetime)): return obj.isoformat()
        return super(ArcaneEncoder, self).default(obj)

def safe_int(val, default=0):
    try:
        if isinstance(val, Decimal): return int(val)
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

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

    headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json'
    }
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
                norm_google,
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
            return (json.dumps(results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)

    elif path == 'categories':
        query = """
            SELECT category, trend_count, avg_category_score
            FROM `dnd-trends-index.gold_data.category_leaders`
            ORDER BY avg_category_score DESC
            LIMIT 10
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)

    elif path == 'news':
        query = """
            SELECT date, headline, hook, body_markdown, key_stat, persona
            FROM `dnd-trends-index.gold_data.daily_articles`
            ORDER BY date DESC, headline ASC
            LIMIT 6
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)
            
    elif path == 'leaderboards':
        source = request.args.get('source', 'google')
        
        if source == 'fandom':
            query = """
                SELECT category, canonical_concept, score as metric_score, top_wiki as source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_fandom_leaderboards`
                ORDER BY heat_score DESC, rank_position ASC;
            """
        elif source == 'wikipedia':
            query = """
                SELECT category, canonical_concept, score as metric_score, 'wikipedia' as source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_wikipedia_leaderboards`
                ORDER BY heat_score DESC, rank_position ASC;
            """
        elif source == 'reddit':
            query = """
                SELECT category, name, metric_score as score, sentiment, history, source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_social_leaderboards`
                ORDER BY heat_score DESC, rank_position ASC;
            """
        elif source == 'bgg':
            query = """
                SELECT category, concept_name as name, owned_count, quality_score, metric_score as score, source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_bgg_leaderboards`
                ORDER BY heat_score DESC, rank_position ASC;
            """
        elif source == 'amazon':
            query = """
                SELECT category, concept_name as name, sales_rank, price, metric_score as score, source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_amazon_leaderboards`
                ORDER BY heat_score DESC, rank_position ASC;
            """
        elif source in ['kickstarter', 'backerkit']:
            query = f"""
                SELECT 
                    'Crowdfunding' as category,
                    name, 
                    velocity, 
                    score_crowdfund * 100 as score, 
                    platform as source,
                    score_crowdfund as heat_score
                FROM `dnd-trends-index.silver_data.norm_crowdfunding`
                WHERE platform = '{source}'
                AND date = (SELECT MAX(date) FROM `dnd-trends-index.silver_data.norm_crowdfunding`)
                ORDER BY score DESC;
            """
        elif source == 'youtube':
            query = """
                SELECT 
                    category, 
                    concept_name as canonical_concept, 
                    consensus_score as metric_score, 
                    creator_count,
                    creator_takes,
                    'youtube' as source,
                    RANK() OVER(PARTITION BY category ORDER BY consensus_score DESC) as rank_position,
                    consensus_score as heat_score
                FROM `dnd-trends-index.gold_data.view_youtube_consensus`
                ORDER BY heat_score DESC, rank_position ASC;
            """
        else:
            query = """
                SELECT category, canonical_concept, google_score_avg as metric_score, 'google' as source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_api_leaderboards`
                ORDER BY heat_score DESC, rank_position ASC;
            """
            
        try:
            results = client.query(query)
            
            ordered_results = []
            category_map = {}
            
            for row in results:
                cat = getattr(row, 'category', 'Uncategorized') or 'Uncategorized'
                if cat not in category_map:
                    category_data = {
                        "category": cat,
                        "heat": safe_float(getattr(row, 'heat_score', 0)),
                        "items": []
                    }
                    ordered_results.append(category_data)
                    category_map[cat] = category_data
                
                if source == 'reddit':
                    item = {
                        "name": str(row.name),
                        "category": str(cat),
                        "score": safe_int(row.score),
                        "sentiment": safe_float(row.sentiment),
                        "rank": safe_int(row.rank_position),
                        "history": [safe_int(x) for x in row.history] if hasattr(row, 'history') and row.history else [0,0,0,0,0,0,0],
                        "source": "reddit"
                    }
                elif source == 'bgg':
                    item = {
                        "name": str(row.name),
                        "category": str(cat),
                        "score": safe_int(row.score),
                        "owned": safe_int(row.owned_count),
                        "quality": safe_float(row.quality_score),
                        "rank": safe_int(row.rank_position),
                        "source": "bgg"
                    }
                elif source == 'amazon':
                    item = {
                        "name": str(row.name),
                        "category": str(cat),
                        "score": safe_int(row.score),
                        "sales_rank": safe_int(row.sales_rank),
                        "price": safe_float(row.price),
                        "formatted_price": f"${safe_float(row.price):,.2f}",
                        "rank": safe_int(row.rank_position),
                        "source": "amazon"
                    }
                elif source in ['kickstarter', 'backerkit']:
                    item = {
                        "name": str(row.name),
                        "category": "Crowdfunding",
                        "score": safe_int(row.score),
                        "velocity": f"${safe_int(row.velocity):,}",
                        "rank": 0,
                        "source": str(row.source)
                    }
                else:
                    item = {
                        "name": getattr(row, 'name', getattr(row, 'canonical_concept', 'Unknown')),
                        "score": safe_float(row.metric_score if hasattr(row, 'metric_score') else 0),
                        "rank": safe_int(row.rank_position if hasattr(row, 'rank_position') else 0),
                        "source": str(getattr(row, 'source', source))
                    }
                    
                    if source == 'youtube':
                        item["consensus_score"] = safe_float(row.metric_score)
                        item["creator_count"] = safe_int(row.creator_count)
                        item["creator_takes"] = [dict(t) for t in row.creator_takes] if row.creator_takes else []
                
                category_map[cat]["items"].append(item)
                
            return (json.dumps(ordered_results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e), "source": source}, cls=ArcaneEncoder), 500, headers)
            
    return (json.dumps({"error": "Endpoint not found"}), 404, headers)
