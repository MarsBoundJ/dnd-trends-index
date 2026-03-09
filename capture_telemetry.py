from google.cloud import bigquery
import json
import datetime
from decimal import Decimal

client = bigquery.Client()

class ArcaneEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
        return super(ArcaneEncoder, self).default(obj)

def get_telemetry():
    queries = {
        "google": "SELECT category, canonical_concept as name, google_score_avg as score FROM `dnd-trends-index.gold_data.view_api_leaderboards` ORDER BY google_score_avg DESC LIMIT 20",
        "fandom": "SELECT category, canonical_concept as name, score FROM `dnd-trends-index.gold_data.view_fandom_leaderboards` ORDER BY score DESC LIMIT 20",
        "wikipedia": "SELECT category, canonical_concept as name, score FROM `dnd-trends-index.gold_data.view_wikipedia_leaderboards` ORDER BY score DESC LIMIT 20",
        "reddit": "SELECT category, name, metric_score as score, sentiment FROM `dnd-trends-index.gold_data.view_social_leaderboards` WHERE source = 'reddit' ORDER BY metric_score DESC LIMIT 20"
    }
    telemetry = {}
    for source, sql in queries.items():
        try:
            results = client.query(sql, location="US")
            telemetry[source] = [dict(row) for row in results]
        except Exception as e:
            telemetry[source] = {"error": str(e)}
    
    print(json.dumps(telemetry, cls=ArcaneEncoder, indent=2))

if __name__ == "__main__":
    get_telemetry()
