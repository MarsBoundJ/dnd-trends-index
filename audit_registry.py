from google.cloud import bigquery
from googleapiclient.discovery import build
import os

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_raw"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.channel_registry"
YOUTUBE_API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

def audit():
    client = bigquery.Client()
    print("--- Channel Registry Audit ---")
    query = f"SELECT * FROM `{REGISTRY_TABLE}`"
    results = client.query(query).to_dataframe()
    print(results.to_string(index=False))

    print("\n--- YouTube API Validation ---")
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    
    # Test with a known channel (Nerd Immersion)
    test_id = "UCi-PqisPTpljX0TUN0N_7gA"
    try:
        res = youtube.channels().list(part="snippet", id=test_id).execute()
        if "items" in res and res["items"]:
            print(f"SUCCESS: Found channel {res['items'][0]['snippet']['title']}")
        else:
            print(f"FAILURE: No channel found for {test_id}. Check API Key permissions.")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    audit()
