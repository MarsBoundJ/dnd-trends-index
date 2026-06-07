
import requests
import os

# Proxy is read from the PROXY_URL environment variable (see .env.example)
proxy_url = os.environ.get("PROXY_URL")

print(f"Testing Proxy (Static User): {proxy_url.split('@')[1]}")

proxies = {
    "http": proxy_url,
    "https": proxy_url,
}

try:
    print("Attempting to fetch ipify.org via proxy...")
    response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=10)
    response.raise_for_status()
    print("Success!")
    print(f"Detected IP: {response.json()['ip']}")
    
except Exception as e:
    print(f"Proxy Connection Failed: {e}")
