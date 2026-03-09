from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

PROJECT_ID = "dnd-trends-index"
KEY_PATH = "/app/dnd-key.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]

def validate_sa():
    if not os.path.exists(KEY_PATH):
        print(f"ERROR: Key file not found at {KEY_PATH}")
        return

    try:
        credentials = service_account.Credentials.from_service_account_file(
            KEY_PATH, scopes=SCOPES
        )
        youtube = build("youtube", "v3", credentials=credentials)
        
        # Test with Nerd Immersion
        test_id = "UCi-PqisPTpljX0TUN0N_7gA"
        res = youtube.channels().list(part="snippet", id=test_id).execute()
        
        if "items" in res and res["items"]:
            print(f"SUCCESS: Service Account can access YouTube. Found: {res['items'][0]['snippet']['title']}")
        else:
            print(f"FAILURE: Service Account failed to find channel {test_id}. Check YouTube API enablement on the project.")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    validate_sa()
