import json
from google.cloud import bigquery
from decimal import Decimal
import datetime
import traceback

class ArcaneEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
        return super(ArcaneEncoder, self).default(obj)

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    try:
        if isinstance(val, Decimal): return int(val)
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default

def run_diagnostic():
    print("Testing Top-Level Vertex AI Init...")
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel
        PROJECT_ID = "dnd-trends-index"
        LOCATION = "us-central1"
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        gemini_model = GenerativeModel("gemini-1.5-flash")
        print("Success: Top-level Vertex AI initialized.")
    except Exception as e:
        print(f"Failed Top-level Vertex AI: {e}")
        traceback.print_exc()

    print("\nTesting BigQuery Summary Logic...")
    client = bigquery.Client(project='dnd-trends-index')
    
    try:
        migration_query = """
            SELECT market_share 
            FROM `dnd-trends-index.gold_data.study_edition_migration` 
            WHERE ruleset_tag = '2024_REVISION' 
            ORDER BY date DESC LIMIT 1
        """
        migration_res = list(client.query(migration_query).result())
        migration_index = safe_float(migration_res[0].market_share) if migration_res else 0.0
        print(f"Success: migration_index={migration_index}")
    except Exception as e:
        print(f"Failed Migration: {e}")
        traceback.print_exc()

    try:
        shortage_query = """
            SELECT demand_ratio 
            FROM `dnd-trends-index.gold_data.study_dm_shortage` 
            ORDER BY date DESC LIMIT 1
        """
        shortage_res = list(client.query(shortage_query).result())
        shortage_index = safe_float(shortage_res[0].demand_ratio) if shortage_res else 0.0
        print(f"Success: shortage_index={shortage_index}")
    except Exception as e:
        print(f"Failed Shortage: {e}")
        traceback.print_exc()

    try:
        gap_query = """
            WITH high_social AS (
                SELECT name FROM `dnd-trends-index.gold_data.view_social_leaderboards` WHERE metric_score > 60
            ),
            market_concepts AS (
                SELECT name FROM `dnd-trends-index.gold_data.view_bgg_leaderboards`
                UNION DISTINCT
                SELECT name FROM `dnd-trends-index.gold_data.view_rpggeek_leaderboards`
                UNION DISTINCT
                SELECT name FROM `dnd-trends-index.gold_data.view_amazon_leaderboards`
            )
            SELECT COUNT(*) as gap_count
            FROM high_social
            WHERE name NOT IN (SELECT name FROM market_concepts)
        """
        gap_res = list(client.query(gap_query).result())
        gap_count = safe_int(gap_res[0].gap_count) if gap_res else 0
        print(f"Success: gap_count={gap_count}")
    except Exception as e:
        print(f"Failed 3P Gap: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_diagnostic()
