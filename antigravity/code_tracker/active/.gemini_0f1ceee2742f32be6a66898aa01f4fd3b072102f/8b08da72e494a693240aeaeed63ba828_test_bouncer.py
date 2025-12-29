À
import requests
import json
import time

def test_api():
    try:
        with open("bouncer_url.txt", "r") as f:
            url = f.read().strip()
    except FileNotFoundError:
        print("bouncer_url.txt not found. Deployment might remain in progress or failed.")
        return

    paths = ["", "/", "/get_daily_trends", "/get_trend_data"]
    
    for path in paths:
        target = url + path
        print(f"--- Testing: {target} ---")
        try:
            r = requests.get(target)
            print(f"Status Code: {r.status_code}")
            if r.status_code == 200:
                print("SUCCESS!")
                print("Headers:", r.headers)
                print("Response:", r.text[:200])
                break # Found it
            else:
                print(f"Error Body: {r.text[:100]}")
        except Exception as e:
            print(f"Failed: {e}")

if __name__ == "__main__":
    test_api()
À"(0f1ceee2742f32be6a66898aa01f4fd3b072102f2.file:///C:/Users/Yorri/.gemini/test_bouncer.py:file:///C:/Users/Yorri/.gemini