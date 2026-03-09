from google.cloud import bigquery
import json
import datetime
from decimal import Decimal
import os

# Initialize BigQuery client
client = bigquery.Client()

class ArcaneEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
        return super(ArcaneEncoder, self).default(obj)

def generate_dump():
    print("Initiating Telemetry Dump...")
    queries = {
        "google": "SELECT category, canonical_concept as name, google_score_avg as score FROM `dnd-trends-index.gold_data.view_api_leaderboards` ORDER BY google_score_avg DESC LIMIT 20",
        "fandom": "SELECT category, canonical_concept as name, score FROM `dnd-trends-index.gold_data.view_fandom_leaderboards` ORDER BY score DESC LIMIT 20",
        "wikipedia": "SELECT category, canonical_concept as name, score FROM `dnd-trends-index.gold_data.view_wikipedia_leaderboards` ORDER BY score DESC LIMIT 20",
        "reddit": "SELECT category, name, metric_score as score, sentiment FROM `dnd-trends-index.gold_data.view_social_leaderboards` WHERE source = 'reddit' ORDER BY metric_score DESC LIMIT 20"
    }
    
    telemetry = {}
    for source, sql in queries.items():
        try:
            print(f"Querying {source}...")
            query_job = client.query(sql, location="US")
            telemetry[source] = [dict(row) for row in query_job]
        except Exception as e:
            print(f"Error querying {source}: {e}")
            telemetry[source] = {"error": str(e)}
            
    output_path = "telemetry_audit_FEB26.json"
    print(f"Writing to {output_path}...")
    with open(output_path, "w") as f:
        json.dump(telemetry, f, cls=ArcaneEncoder, indent=2)
    print("Dump Complete.")

if __name__ == "__main__":
    generate_dump()
