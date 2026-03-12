
import functions_framework
from google.cloud import bigquery
import json
import datetime
import os
import vertexai
from vertexai.generative_models import GenerativeModel

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"

def get_gemini_model():
    """Lazily initialize and return the Gemini model."""
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        return GenerativeModel("gemini-1.5-flash")
    except Exception as e:
        print(f"FAILED TO INITIALIZE VERTEX AI: {e}")
        return None

from decimal import Decimal
from decimal import Decimal

class ArcaneEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
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
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type, X-Ritual-Key',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-Ritual-Key',
        'Content-Type': 'application/json'
    }
    client = bigquery.Client(project='dnd-trends-index')
    print(f"DEBUG: Path={request.path}, FullPath={request.full_path}, Args={request.args}")
    
    # Stable Router for GCF
    # Support root trigger, /leaderboards, or various path prefixes
    try:
        full_path = request.path
        # Flexible matching: if 'health' or 'telemetry' is in the path, use that. Otherwise default to leaderboards.
        if full_path.endswith('suggestions/update'):
            path = 'system/suggestions/update'
        elif full_path.endswith('suggestions/pending'):
            path = 'system/suggestions/pending'
        elif full_path.endswith('ingest-catalog'):
            path = 'system/library/ingest-catalog'
        elif full_path.endswith('enrich'):
            path = 'system/library/enrich'
        elif full_path.endswith('update'):
            path = 'system/library/update'
        elif full_path.endswith('search'):
            path = 'system/library/search'
        elif 'health' in full_path:
            path = 'system/health'
        elif 'telemetry' in full_path:
            path = 'system/telemetry'
        elif 'vista/summary' in full_path:
            path = 'system/vista/summary'
        elif 'vista/dm-shortage' in full_path:
            path = 'system/vista/dm-shortage'
        elif 'vista/momentum' in full_path:
            path = 'system/vista/momentum'
        elif 'analyst/chat' in full_path:
            path = 'system/analyst/chat'
        elif 'analyst/briefing' in full_path:
            path = 'system/analyst/briefing'
        elif 'vista/email-report' in full_path:
            path = 'system/vista/email-report'
        elif 'vista/chart_data' in full_path:
            path = 'system/vista/chart_data'
        else:
            path = 'leaderboards'
        
        print(f"DEBUG: Decided Path={path} from full_path={full_path}")
    except Exception as e:
        print(f"Routing Error: {str(e)}")
        path = 'leaderboards'

    if path == 'leaderboards_legacy':
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
        elif source == 'rpggeek':
            query = """
                SELECT category, concept_name as name, owned_count, quality_score, metric_score as score, source, rank_position, heat_score
                FROM `dnd-trends-index.gold_data.view_rpggeek_leaderboards`
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
                WITH last_7_days AS (
                    SELECT l.category, m.canonical_concept, t.date, t.interest 
                    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
                    JOIN `dnd-trends-index.silver_data.view_google_mapping` m USING (term_id)
                    JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` l ON m.canonical_concept = l.concept_name
                    WHERE t.date >= DATE_SUB((SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`), INTERVAL 7 DAY) 
                    AND l.is_active = TRUE
                ),
                item_stats AS (
                    SELECT category, canonical_concept,
                        AVG(interest) as metric_score,
                        SUM(IF(date = (SELECT MAX(date) FROM last_7_days), interest, 0)) as current_score
                    FROM last_7_days GROUP BY 1, 2
                ),
                ranked_items AS (
                    SELECT category, canonical_concept, metric_score, current_score,
                        ROW_NUMBER() OVER(PARTITION BY category ORDER BY metric_score DESC) as rank_position
                    FROM item_stats
                ),
                category_heat AS (
                    SELECT category, AVG(metric_score) as heat_score
                    FROM ranked_items WHERE rank_position <= 20
                    GROUP BY 1
                ),
                gaps AS (
                    SELECT concept_name, opportunity_index 
                    FROM `dnd-trends-index.gold_data.study_market_gaps`
                )
                SELECT 
                    r.category, 
                    r.canonical_concept, 
                    r.metric_score, 
                    r.current_score, 
                    'google' as source, 
                    r.rank_position, 
                    c.heat_score,
                    COALESCE(g.opportunity_index, 0) as opportunity_index
                FROM ranked_items r
                JOIN category_heat c USING (category)
                LEFT JOIN gaps g ON LOWER(r.canonical_concept) = LOWER(g.concept_name)
                WHERE r.rank_position <= 40
                ORDER BY c.heat_score DESC, r.rank_position ASC
            """

            
        try:
            client = bigquery.Client(project='dnd-trends-index')
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
                elif source in ['bgg', 'rpggeek']:
                    item = {
                        "name": str(row.name),
                        "category": str(cat),
                        "score": safe_int(row.score),
                        "owned": safe_int(row.owned_count),
                        "quality": safe_float(row.quality_score),
                        "rank": safe_int(row.rank_position),
                        "source": str(row.source)
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
                        "current_score": safe_float(getattr(row, 'current_score', 0)),
                        "rank": safe_int(row.rank_position if hasattr(row, 'rank_position') else 0),
                        "source": str(getattr(row, 'source', source)),
                        "opportunity_index": safe_float(getattr(row, 'opportunity_index', 0))
                    }

                    
                    if source == 'youtube':
                        item["consensus_score"] = safe_float(row.metric_score)
                        item["creator_count"] = safe_int(row.creator_count)
                        item["creator_takes"] = [dict(t) for t in row.creator_takes] if row.creator_takes else []
                
                category_map[cat]["items"].append(item)
                
            return (json.dumps(ordered_results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e), "source": source}, cls=ArcaneEncoder), 500, headers)
            
    elif path == 'system/health':
        # Fetch Scheduler Metadata (Mocked/Static for now as Cloud Scheduler API access is complex via CF)
        # In a real scenario, we'd use the Cloud Scheduler client library.
        # We will assume a 24h cycle for the 'Caldean' trigger.
        now = datetime.datetime.now()
        caldean_cycle = {
            "last_run": (now - datetime.timedelta(hours=4)).isoformat(),
            "next_run": (now + datetime.timedelta(hours=20)).isoformat(),
            "mode": "Mercury Active" if now.weekday() < 5 else "Shabbat Pause"
        }

        query = """
            WITH all_tables AS (
                SELECT 'google' as source_name, 'trend_data_pilot' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_categorized.__TABLES__` WHERE table_id = 'trend_data_pilot'
                UNION ALL
                SELECT 'fandom' as source_name, 'fandom_daily_metrics' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'fandom_daily_metrics'
                UNION ALL
                SELECT 'wikipedia' as source_name, 'wikipedia_daily_views' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.social_data.__TABLES__` WHERE table_id = 'wikipedia_daily_views'
                UNION ALL
                SELECT 'reddit' as source_name, 'reddit_daily_metrics' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_categorized.__TABLES__` WHERE table_id = 'reddit_daily_metrics'
                UNION ALL
                SELECT 'youtube' as source_name, 'yt_video_index' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'yt_video_index'
                UNION ALL
                SELECT 'bgg' as source_name, 'bgg_product_stats' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'bgg_product_stats'
                UNION ALL
                SELECT 'rpggeek' as source_name, 'rpggeek_product_stats' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'rpggeek_product_stats'
                UNION ALL
                SELECT 'amazon' as source_name, 'amazon_daily_stats' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'amazon_daily_stats'
                UNION ALL
                SELECT 'kickstarter' as source_name, 'kickstarter_projects' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.commercial_data.__TABLES__` WHERE table_id = 'kickstarter_projects'
                UNION ALL
                SELECT 'backerkit' as source_name, 'backerkit_projects' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.commercial_data.__TABLES__` WHERE table_id = 'backerkit_projects'
                UNION ALL
                SELECT 'catalog_supply' as source_name, 'catalog_supply' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'catalog_supply'
                UNION ALL
                SELECT 'itchio_products' as source_name, 'itchio_products' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'itchio_products'
                UNION ALL
                SELECT 'itchio_jams' as source_name, 'itchio_jams' as table_id, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time, row_count FROM `dnd-trends-index.dnd_trends_raw.__TABLES__` WHERE table_id = 'itchio_jams'
            )
            SELECT 
                source_name,
                table_id,
                last_modified_time,
                row_count,
                CASE 
                    WHEN source_name IN ('wikipedia', 'youtube') THEN
                        IF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_time, HOUR) < 120, '🟢 HEALTHY', '🔴 STALE')
                    ELSE
                        IF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_time, HOUR) < 24, '🟢 HEALTHY', '🔴 STALE')
                END as status
            FROM all_tables
        """
        try:
            results = client.query(query)
            sources_health = {}
            for row in results:
                sources_health[row.source_name] = {
                    "table_id": row.table_id,
                    "status": row.status,
                    "last_modified": row.last_modified_time,
                    "row_count": safe_int(row.row_count)
                }
            
            # Ensure all sources are represented
            expected_sources = ['google', 'fandom', 'wikipedia', 'reddit', 'youtube', 'bgg', 'amazon', 'kickstarter', 'backerkit', 'catalog_supply', 'itchio_products', 'itchio_jams']

            for src in expected_sources:
                if src not in sources_health:
                    sources_health[src] = {"status": "🔴 STALE", "last_modified": None, "row_count": 0}

            response_data = {
                "caldean_cycle": caldean_cycle,
                "sources": sources_health
            }

            return (json.dumps(response_data, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)

    elif path == 'system/telemetry':
        queries = {
            "google": "SELECT category, canonical_concept as name, google_score_avg as score FROM `dnd-trends-index.gold_data.view_api_leaderboards` ORDER BY google_score_avg DESC LIMIT 20",
            "fandom": "SELECT category, canonical_concept as name, score FROM `dnd-trends-index.gold_data.view_fandom_leaderboards` ORDER BY score DESC LIMIT 20",
            "wikipedia": "SELECT category, canonical_concept as name, score FROM `dnd-trends-index.gold_data.view_wikipedia_leaderboards` ORDER BY score DESC LIMIT 20",
            "reddit": "SELECT category, name, metric_score as score, sentiment FROM `dnd-trends-index.gold_data.view_social_leaderboards` WHERE source = 'reddit' ORDER BY metric_score DESC LIMIT 20"
        }
        telemetry = {}
        try:
            client = bigquery.Client(project='dnd-trends-index')
            for source, sql in queries.items():
                results = client.query(sql)
                telemetry[source] = [dict(row) for row in results]
            return (json.dumps(telemetry, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)

    elif path == 'system/library/search':
        q = request.args.get('q', '')
        if not q:
            return (json.dumps([]), 200, headers)
        
        query = f"""
            SELECT concept_name as name, category, is_active
            FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
            WHERE LOWER(concept_name) LIKE LOWER('%{q}%')
            ORDER BY is_active DESC, concept_name ASC
            LIMIT 50
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)

    elif path == 'system/library/update':
        if request.method != 'POST':
            return (json.dumps({"error": "Method not allowed"}), 405, headers)
        
        try:
            data = request.get_json(silent=True)
            concept_name = data.get('concept_name')
            category = data.get('category')
            is_active = data.get('is_active', True)
            
            if not concept_name:
                return (json.dumps({"error": "Missing concept_name"}), 400, headers)
            
            # Use query parameters to prevent injection if possible, but for BQ scripts it's often easier this way
            # We will use job_config to be safe
            sql = """
                UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
                SET category = @category, is_active = @is_active
                WHERE concept_name = @concept_name
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("category", "STRING", category),
                    bigquery.ScalarQueryParameter("is_active", "BOOL", is_active),
                    bigquery.ScalarQueryParameter("concept_name", "STRING", concept_name),
                ]
            )
            client.query(sql, job_config=job_config).result()
            return (json.dumps({"status": "success", "concept": concept_name}), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)


    elif path == 'system/vista/summary':
        try:
            client = bigquery.Client(project='dnd-trends-index')
            # 1. Migration Index
            # 1. Edition Migration Index
            try:
                migration_query = """
                    SELECT market_share 
                    FROM `dnd-trends-index.gold_data.study_edition_migration` 
                    WHERE ruleset_tag = '2024_REVISION' 
                    ORDER BY date DESC LIMIT 1
                """
                migration_res = list(client.query(migration_query).result())
                migration_index = safe_float(migration_res[0].market_share) if migration_res else 0.0
                print(f"DEBUG: migration_index={migration_index}")
            except Exception as e:
                migration_index = 0.0
                print(f"Error calculating Migration: {e}")

            # 2. DM Shortage Index
            try:
                shortage_query = """
                    SELECT demand_ratio 
                    FROM `dnd-trends-index.gold_data.study_dm_shortage` 
                    ORDER BY date DESC LIMIT 1
                """
                shortage_res = list(client.query(shortage_query).result())
                shortage_index = safe_float(shortage_res[0].demand_ratio) if shortage_res else 0.0
                print(f"DEBUG: shortage_index={shortage_index}")
            except Exception as e:
                shortage_index = 0.0
                print(f"Error calculating Shortage: {e}")

            # 3. 3P Market Share (Task 4: 2024 vs 5e Legacy Ratio)
            gap_count = "0.0%"
            try:
                ms_query = """
                    SELECT 
                        COUNTIF(edition_tag = '2024') as count_2024,
                        COUNTIF(edition_tag IN ('5e', 'Legacy')) as count_legacy
                    FROM `dnd-trends-index.dnd_trends_raw.catalog_supply`
                    WHERE source = 'DMs Guild'
                """
                ms_res = list(client.query(ms_query).result())
                if ms_res:
                    c_2024 = ms_res[0].count_2024 or 0
                    c_leg = ms_res[0].count_legacy or 0
                    if c_leg + c_2024 > 0:
                        ratio_val = c_2024 / (c_leg + c_2024)
                        gap_count = f"{ratio_val*100:.1f}%"
                print(f"DEBUG: 3p_market_share={gap_count}")
            except Exception as e:
                print(f"Error calculating 3P Market Share: {e}")

            # 4. Positive Sentiment (Normalized)
            sentiment = 0.0
            try:
                sent_query = "SELECT AVG(SAFE_DIVIDE(sentiment, heat_score)) as avg_sent FROM `dnd-trends-index.gold_data.view_social_leaderboards` WHERE source = 'reddit' AND heat_score > 0"
                sent_res = list(client.query(sent_query).result())
                sentiment = safe_float(sent_res[0].avg_sent) if sent_res else 0.0
                print(f"DEBUG: normalized_sentiment={sentiment}")
            except Exception as e:
                print(f"Error calculating Sentiment: {e}")

            response_data = {
                "migration_index": migration_index,
                "dm_shortage_index": shortage_index,
                "gap_3p_count": gap_count,
                "positive_sentiment": sentiment
            }
            return (json.dumps(response_data, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}, cls=ArcaneEncoder), 500, headers)

    elif path == 'system/vista/dm-shortage':
        try:
            client = bigquery.Client(project='dnd-trends-index')
            query = """
                WITH current_metrics AS (
                    SELECT 'PLAYER' as persona, player_interest as interest
                    FROM `dnd-trends-index.gold_data.study_dm_shortage`
                    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.gold_data.study_dm_shortage`)
                    UNION ALL
                    SELECT 'DM' as persona, dm_interest as interest
                    FROM `dnd-trends-index.gold_data.study_dm_shortage`
                    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.gold_data.study_dm_shortage`)
                ),
                last_year_metrics AS (
                    SELECT 'PLAYER' as persona, player_interest as interest
                    FROM `dnd-trends-index.gold_data.study_dm_shortage`
                    WHERE date = (
                        SELECT MAX(date) FROM `dnd-trends-index.gold_data.study_dm_shortage`
                        WHERE date <= DATE_SUB((SELECT MAX(date) FROM `dnd-trends-index.gold_data.study_dm_shortage`), INTERVAL 1 YEAR)
                    )
                    UNION ALL
                    SELECT 'DM' as persona, dm_interest as interest
                    FROM `dnd-trends-index.gold_data.study_dm_shortage`
                    WHERE date = (
                        SELECT MAX(date) FROM `dnd-trends-index.gold_data.study_dm_shortage`
                        WHERE date <= DATE_SUB((SELECT MAX(date) FROM `dnd-trends-index.gold_data.study_dm_shortage`), INTERVAL 1 YEAR)
                    )
                )
                SELECT 
                    c.persona,
                    c.interest as current_interest,
                    l.interest as last_year_interest,
                    SAFE_DIVIDE(c.interest - l.interest, l.interest) as yoy_growth
                FROM current_metrics c
                JOIN last_year_metrics l USING (persona)
            """
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            print(f"Error in dm-shortage: {e}")
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'system/vista/momentum':
        try:
            client = bigquery.Client(project='dnd-trends-index')
            # Fetch top 5 movers
            movers_query = """
                WITH recent_trends AS (
                    SELECT m.canonical_concept as concept_name, t.date, SUM(t.interest) as interest
                    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
                    JOIN `dnd-trends-index.silver_data.view_google_mapping` m USING (term_id)
                    WHERE t.date >= DATE_SUB((SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`), INTERVAL 7 DAY)
                    GROUP BY 1, 2
                ),
                velocity AS (
                    SELECT concept_name,
                        SUM(IF(date = (SELECT MAX(date) FROM recent_trends), interest, 0)) - 
                        SUM(IF(date = (SELECT MIN(date) FROM recent_trends), interest, 0)) as delta
                    FROM recent_trends
                    GROUP BY 1
                    ORDER BY delta DESC
                    LIMIT 5
                )
                SELECT r.concept_name, r.date, r.interest
                FROM recent_trends r
                JOIN velocity v USING (concept_name)
                ORDER BY r.concept_name, r.date ASC
            """
            rows = list(client.query(movers_query).result())
            
            momentum_data = {}
            for row in rows:
                if row.concept_name not in momentum_data:
                    momentum_data[row.concept_name] = []
                momentum_data[row.concept_name].append(float(row.interest))
            
            # Convert to list for easier frontend consumption
            final_data = [{"concept": k, "trend": v} for k, v in momentum_data.items()]
            return (json.dumps(final_data, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            print(f"Error in momentum: {e}")
            return (json.dumps({"error": str(e)}), 500, headers)
            
    if path == 'system/vista/chart_data':
        try:
            client = bigquery.Client(project='dnd-trends-index')
            chart_query = """
                SELECT date, ruleset_tag, market_share
                FROM `dnd-trends-index.gold_data.study_edition_migration`
                ORDER BY date ASC
            """
            chart_res = client.query(chart_query).result()
            dates = []
            ruleset_2024 = {}
            ruleset_legacy = {}
            
            for row in chart_res:
                date_str = row.date.strftime('%Y-%m-%d')
                if date_str not in dates:
                    dates.append(date_str)
                if row.ruleset_tag == '2024_REVISION':
                    ruleset_2024[date_str] = row.market_share
                else:
                    ruleset_legacy[date_str] = row.market_share
            
            # Formatting for Chart.js
            data_2024 = [float(ruleset_2024.get(d) or 0.0)*100 for d in dates]
            data_legacy = [float(ruleset_legacy.get(d) or 0.0)*100 for d in dates]
            
            return (json.dumps({
                "labels": dates[-30:],  # Last 30 days
                "dataset_2024": data_2024[-30:],
                "dataset_legacy": data_legacy[-30:]
            }), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)
            
    if path == 'system/analyst/chat':
        if request.method != 'POST':
            return (json.dumps({"error": "Method not allowed"}), 405, headers)
            
        try:
            req_data = request.get_json(silent=True)
            user_msg = req_data.get('message', '') if req_data else ''
            
            if not user_msg:
                return (json.dumps({"error": "No message provided"}), 400, headers)
                
            # Fetch latest telemetry for context
            migration_res = list(client.query("SELECT market_share FROM `dnd-trends-index.gold_data.study_edition_migration` WHERE ruleset_tag = '2024_REVISION' ORDER BY date DESC LIMIT 1").result())
            mig_val = migration_res[0].market_share if migration_res and migration_res[0].market_share is not None else 0.0
            
            shortage_res = list(client.query("SELECT demand_ratio FROM `dnd-trends-index.gold_data.study_dm_shortage` ORDER BY date DESC LIMIT 1").result())
            short_val = shortage_res[0].demand_ratio if shortage_res and shortage_res[0].demand_ratio is not None else 0.0
            
            # Get Top 5 Mover concepts
            tide_query = """
                WITH recent_trends AS (
                    SELECT m.canonical_concept as concept_name, t.date as trend_date, SUM(t.interest) as interest
                    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
                    JOIN `dnd-trends-index.silver_data.view_google_mapping` m USING (term_id)
                    WHERE t.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
                    GROUP BY 1, 2
                ),
                velocity_calc AS (
                    SELECT concept_name,
                        SUM(IF(trend_date = (SELECT MAX(trend_date) FROM recent_trends), interest, 0)) - 
                        SUM(IF(trend_date = (SELECT MIN(trend_date) FROM recent_trends), interest, 0)) as velocity
                    FROM recent_trends GROUP BY 1
                )
                SELECT concept_name, velocity FROM velocity_calc ORDER BY velocity DESC LIMIT 5
            """
            tide_res = list(client.query(tide_query).result())
            top_movers = [f"{r.concept_name} (+{r.velocity})" for r in tide_res]
            
            # 4. Decay Signatures (Study #2)
            try:
                decay_query = """
                    SELECT canonical_concept, half_life_days 
                    FROM `dnd-trends-index.gold_data.study_decay_signatures` 
                    WHERE event_name = '2024 PHB Release'
                    ORDER BY half_life_days ASC LIMIT 3
                """
                decay_res = list(client.query(decay_query).result())
                decay_str = ", ".join([f"{r.canonical_concept} ({r.half_life_days}d)" for r in decay_res])
            except Exception as e:
                decay_str = "N/A"
                print("Error fetching decay signatures:", e)

            # 5. Subclass Graveyard (Study #3)
            try:
                graveyard_query = """
                    SELECT subclass_name, subclass_share 
                    FROM `dnd-trends-index.gold_data.study_subclass_graveyard` 
                    WHERE subclass_share < 0.02
                    ORDER BY subclass_share ASC LIMIT 5
                """
                graveyard_res = list(client.query(graveyard_query).result())
                graveyard_str = ", ".join([f"{r.subclass_name} ({r.subclass_share*100:.1f}%)" for r in graveyard_res])
            except Exception as e:
                graveyard_str = "N/A"
                print("Error fetching graveyard subclasses:", e)

            # 6. Market Equilibrium (Study #4 - Phase 71)
            try:
                eq_query = """
                    SELECT concept_name, classification 
                    FROM `dnd-trends-index.gold_data.study_market_equilibrium` 
                    WHERE classification IN ('Hype Bubble', 'Quiet Giant')
                    ORDER BY hype_ratio DESC LIMIT 10
                """
                eq_res = list(client.query(eq_query).result())
                bubbles = [r.concept_name for r in eq_res if r.classification == 'Hype Bubble'][:3]
                giants = [r.concept_name for r in eq_res if r.classification == 'Quiet Giant'][:3]
                equilibrium_str = f"Hype Bubbles: {', '.join(bubbles) if bubbles else 'None'}; Quiet Giants: {', '.join(giants) if giants else 'None'}"
            except Exception as e:
                equilibrium_str = "N/A"
                print("Error fetching equilibrium data:", e)

            # 7. Market Gaps (Study #5 - Phase 75)
            try:
                gap_query = """
                    SELECT concept_name, opportunity_index 
                    FROM `dnd-trends-index.gold_data.study_market_gaps` 
                    WHERE gap_rank <= 5
                    ORDER BY gap_rank ASC
                """
                gap_res = list(client.query(gap_query).result())
                gap_str = ", ".join([f"{r.concept_name} (Gap Index: {r.opportunity_index:.1f})" for r in gap_res])
            except Exception as e:
                gap_str = "N/A"
                print("Error fetching market gaps:", e)

            page_data = f"""
            START OF DND TRENDS TELEMETRY:
            - Edition Migration (2024 Rules Adoption): {float(mig_val)*100:.1f}%
            - DM Deficit Ratio (Player Interest vs DM Interest): {float(short_val):.2f}x
            - Top 5 Movers (7-Day Velocity): {', '.join(top_movers)}
            - Decay Signatures (2024 PHB): {decay_str}
            - Subclass Graveyard (<2% share): {graveyard_str}
            - Market Equilibrium (Hype vs Sales): {equilibrium_str}
            - Top 5 Market Gaps (Study #5): {gap_str}
            END OF DND TRENDS TELEMETRY.
            """

            
            system_prompt = f"""
            You are the Arcane Analyst — a senior strategic intelligence advisor at Wizards of the Coast. 
            You combine McKinsey-level precision with deep D&D fluency. Cite specific numbers from the provided context. 
            Focus on business implications: revenue opportunities, edition adoption, and design gaps. 
            Use D&D vocabulary naturally but professionally. Your responses should be formatted clearly.
            
            CONTEXT:
            {page_data}
            """
            
            model = get_gemini_model()
            if not model:
                return (json.dumps({"error": "Analyst is currently offline (Vertex AI Init Failure)"}), 503, headers)

            response = model.generate_content(
                f"{system_prompt}\n\nUSER INQUIRY: {user_msg}"
            )
            
            return (json.dumps({"reply": response.text}), 200, headers)
            
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'system/analyst/briefing':
        if request.method != 'POST':
            return (json.dumps({"error": "Method not allowed"}), 405, headers)
        try:
            req_data = request.get_json(silent=True)
            chat_history = req_data.get('chat_history', []) if req_data else []
            
            if not chat_history:
                return (json.dumps({"error": "No chat history provided"}), 400, headers)
            
            # Format chat history for prompt
            chat_transcript = ""
            for msg in chat_history:
                role = "User" if msg.get("role") == "user" else "Arcane Analyst"
                chat_transcript += f"{role}: {msg.get('content')}\n"
            
            summary_prompt = f"""
            Summarize the preceding conversation into a one-page executive briefing. 
            Use headers: EXECUTIVE SUMMARY, DATA EVIDENCE, and RECOMMENDED ACTIONS. 
            Maintain the McKinsey-advisor tone.

            CONVERSATION TRANSCRIPT:
            {chat_transcript}
            """
            
            model = get_gemini_model()
            if not model:
                return (json.dumps({"error": "Scribe is currently unavailable"}), 503, headers)

            response = model.generate_content(summary_prompt)
            briefing_text = response.text
            
            # Save to BQ
            import uuid
            session_id = str(uuid.uuid4())
            curr_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            hist_str = json.dumps(chat_history).replace("'", "''")
            brief_str = briefing_text.replace("'", "''")
            
            insert_query = f"""
                INSERT INTO `dnd-trends-index.gold_data.chat_archives` 
                (session_id, user_email, chat_history, summary_text, created_at)
                VALUES ('{session_id}', 'anonymous', JSON'{hist_str}', '{brief_str}', TIMESTAMP('{curr_time}'))
            """
            client.query(insert_query).result()
            
            return (json.dumps({"summary_text": briefing_text}), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)
            
    if path == 'system/vista/email-report':
        if request.method != 'POST':
            return (json.dumps({"error": "Method not allowed"}), 405, headers)
        try:
            req_data = request.get_json(silent=True)
            email = req_data.get('email', '') if req_data else ''
            summary = req_data.get('summary', '') if req_data else ''
            
            if not email:
                return (json.dumps({"error": "No email provided"}), 400, headers)
                
            # PLACEHOLDER: Integrate SendGrid or equivalent here.
            print(f"[EMAIL MOCK] Sending Briefing to {email}. Content length: {len(summary)}")
            
            return (json.dumps({"status": "success", "message": f"Email dispatched to {email}"}), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'system/library/enrich':
        if request.method != 'POST':
            return (json.dumps({"error": "Method not allowed"}), 405, headers)
        try:
            req_data = request.get_json(silent=True)
            if not req_data or not isinstance(req_data, list):
                return (json.dumps({"error": "Expected JSON array of items"}), 400, headers)
                
            model = get_gemini_model()
            if not model:
                return (json.dumps({"error": "AI Librarian offline"}), 503, headers)
                
            enriched_items = []
            for item in req_data:
                title = item.get("title", "")
                snippet = item.get("snippet", "")
                
                if not title:
                    enriched_items.append({"publisher": "Unknown", "primary_category": "Other", "summary": ""})
                    continue
                    
                prompt = f"""
                You are the Arcane Librarian, an expert in TTRPG publishing. Given the Title: '{title}' and Snippet: '{snippet}', return a JSON object with:
                'publisher' (Who made it?),
                'primary_category' (Choose: Adventure, Setting, Monsters, Player Options, Rules, or Accessory),
                'summary' (1 sentence of what it's about).
                If you are unsure, provide your best guess based on your training data. ONLY output valid JSON.
                """
                
                try:
                    response = model.generate_content(prompt)
                    res_text = response.text.strip()
                    if res_text.startswith("```json"): res_text = res_text[7:]
                    if res_text.startswith("```"): res_text = res_text[3:]
                    if res_text.endswith("```"): res_text = res_text[:-3]
                    
                    parsed_res = json.loads(res_text.strip())
                    enriched_items.append(parsed_res)
                except Exception as e:
                    print(f"Gemini Inference Error for {title}: {e}")
                    enriched_items.append({"publisher": "Unknown", "primary_category": "Other", "summary": ""})
                    
            return (json.dumps(enriched_items), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'system/suggestions/pending':
        query = """
            SELECT concept_name, suggested_category, reason, status
            FROM `dnd-trends-index.dnd_trends_raw.ai_suggestions`
            WHERE status = 'PENDING'
            ORDER BY concept_name
        """
        try:
            results = [dict(row) for row in client.query(query)]
            return (json.dumps(results, cls=ArcaneEncoder), 200, headers)
        except Exception as e:
            return (json.dumps({"error": str(e)}), 500, headers)

    elif path == 'system/suggestions/update':
        if request.method != 'POST':
            return (json.dumps({"error": "POST required"}), 405, headers)
        body = request.get_json()
        concept_name = body.get('concept_name', '')
        action = body.get('action', '')
        new_category = body.get('category', '')

        if action == 'APPROVE' and new_category:
            # 1. Update status in ai_suggestions
            update_query = """
                UPDATE `dnd-trends-index.dnd_trends_raw.ai_suggestions`
                SET status = 'APPROVED'
                WHERE concept_name = @concept_name
            """
            
            # 2. UPSERT into concept_library
            library_query = """
                MERGE `dnd-trends-index.dnd_trends_categorized.concept_library` T
                USING (SELECT @concept_name AS concept_name, @new_category AS category) S
                ON T.concept_name = S.concept_name
                WHEN MATCHED THEN
                  UPDATE SET category = S.category, last_processed_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                  INSERT (concept_name, category, last_processed_at, is_active)
                  VALUES (S.concept_name, S.category, CURRENT_TIMESTAMP(), true)
            """
            
            # We can use a single job_config if we use the same parameters in both or separate them
            # To be safest with the user's previous "unreferenced parameter" issue, let's stick to separate configs or a unified one for MERGE
            job_config_suggestions = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("concept_name", "STRING", concept_name),
            ])
            job_config_library = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("concept_name", "STRING", concept_name),
                bigquery.ScalarQueryParameter("new_category", "STRING", new_category),
            ])
            
            try:
                client.query(update_query, job_config=job_config_suggestions).result()
                client.query(library_query, job_config=job_config_library).result()
                return (json.dumps({"status": "approved", "concept": concept_name}), 200, headers)
            except Exception as e:
                print(f"ERROR in suggestions/update (APPROVE): {e}")
                return (json.dumps({"error": str(e)}), 500, headers)

        elif action in ('REJECT', 'ARCHIVE'):
            update_query = """
                UPDATE `dnd-trends-index.dnd_trends_raw.ai_suggestions`
                SET status = @action
                WHERE concept_name = @concept_name
            """
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("concept_name", "STRING", concept_name),
                bigquery.ScalarQueryParameter("action", "STRING", action),
            ])
            try:
                client.query(update_query, job_config=job_config).result()
                return (json.dumps({"status": action.lower(), "concept": concept_name}), 200, headers)
            except Exception as e:
                return (json.dumps({"error": str(e)}), 500, headers)

        return (json.dumps({"error": "Invalid action"}), 400, headers)

    elif path == 'system/library/ingest-catalog':
        if request.method != 'POST':
            return (json.dumps({"error": "POST required"}), 405, headers)
        ritual_key = request.headers.get('X-Ritual-Key', '')
        if ritual_key != 'ArcaneLibrarian2026':
            return (json.dumps({"error": "Unauthorized"}), 403, headers)
        rows = request.get_json()
        if not rows:
            return (json.dumps({"error": "No data"}), 400, headers)
        errors = client.insert_rows_json('dnd-trends-index.dnd_trends_raw.catalog_supply', rows)
        if errors:
            return (json.dumps({"error": str(errors)}), 500, headers)
        return (json.dumps({"inserted": len(rows)}), 200, headers)

    elif path == 'system/library/enrich':
        if request.method != 'POST':
            return (json.dumps({"error": "POST required"}), 405, headers)
        items = request.get_json()
        if not items or not isinstance(items, list):
            return (json.dumps({"error": "Invalid input, expected list of product titles/snippets"}), 400, headers)
        
        model = get_gemini_model()
        if not model:
            return (json.dumps({"error": "Vertex AI Init Failure"}), 500, headers)
            
        enriched_results = []
        for item in items:
            title = item.get('title', 'Unknown Title')
            snippet = item.get('snippet', '')
            prompt = f"""
            Analyze this TTRPG product:
            Title: {title}
            Snippet: {snippet}

            Return a RAW JSON object with:
            - publisher (string, use 'Universal' if unknown)
            - primary_category (string from: Monster, Spell, Mechanic, Class, Subclass, Feat, Species & Lineage, Background, Deity, Party Role, Equipment, Location, NPC, PC, Faction, Lore, Art, Accessory, Rulebooks, Setting, TTRPG System)
            - summary (concise 1-sentence description)

            Rules:
            - ONLY return the JSON. 
            - No markdown blocks.
            """
            try:
                # Use generate_content for quick batching (ideally batch but simple loop for now)
                response = model.generate_content(prompt)
                raw_text = response.text.replace('```json', '').replace('```', '').strip()
                enriched_results.append(json.loads(raw_text))
            except Exception as e:
                print(f"Enrichment error for {title}: {e}")
                enriched_results.append({
                    "publisher": "Unknown",
                    "primary_category": "Uncategorized",
                    "summary": "Error during enrichment"
                })
        
        return (json.dumps(enriched_results, cls=ArcaneEncoder), 200, headers)

    return (json.dumps({"error": "Endpoint not found"}), 404, headers)

