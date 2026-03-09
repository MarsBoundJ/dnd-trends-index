from google.cloud import bigquery
import json
import logging

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
DATASET_ID = "dnd_trends_raw"
INTELLIGENCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_intelligence"

def check_data():
    client = bigquery.Client()
    query = f"""
    SELECT 
        video_id, 
        concept_name, 
        sentiment_label, 
        verdict, 
        reported_not_creator 
    FROM `{INTELLIGENCE_TABLE}` 
    LIMIT 5
    """
    try:
        results = client.query(query).result()
        report = []
        for row in results:
            report.append(dict(row))
        
        if not report:
            print("STATUS: TABLE_EMPTY")
        else:
            print("STATUS: DATA_FOUND")
            print(json.dumps(report, indent=2))
    except Exception as e:
        if "Not found" in str(e):
            print("STATUS: TABLE_MISSING")
        else:
            print(f"STATUS: ERROR - {e}")

if __name__ == "__main__":
    check_data()
