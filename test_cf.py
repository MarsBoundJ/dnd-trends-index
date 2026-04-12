import requests
import subprocess
import json
import os

def get_token():
    return subprocess.check_output("gcloud auth print-identity-token", shell=True).decode("utf-8").strip()

url = "https://us-central1-dnd-trends-index.cloudfunctions.net/monster-classifier"
try:
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Live run: dry_run=False, no max_batches limit
    data = {
        "dry_run": False
    }

    print(f"Triggering live run at {url}...")
    response = requests.post(url, headers=headers, json=data, timeout=600) # 10 minute timeout for the request
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"Error: {e}")
