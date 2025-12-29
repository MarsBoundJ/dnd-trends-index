
import requests
import json
import time

URL = "https://dnd-daily-journalist-kfh5mgjgiq-uc.a.run.app"

def generate_batch():
    batch = [
        ("The Goblin", 3)
    ]
    
    for persona, count in batch:
        print(f"\n--- Generating {count} articles for {persona} ---")
        for i in range(count):
            print(f"Request {i+1}...")
            try:
                response = requests.post(URL, json={"persona": persona}, timeout=300)
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ Success: {data['article']['headline']}")
                    # Small sleep to allow BQ timestamps to slightly differ and avoid rate limits
                    time.sleep(2)
                else:
                    print(f"❌ Error {response.status_code}: {response.text}")
            except Exception as e:
                print(f"❌ Failed: {e}")

if __name__ == "__main__":
    generate_batch()
