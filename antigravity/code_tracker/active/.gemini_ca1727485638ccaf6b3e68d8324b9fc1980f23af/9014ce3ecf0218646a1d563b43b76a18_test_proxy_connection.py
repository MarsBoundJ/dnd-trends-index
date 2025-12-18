ÿ
import requests
import os
# Manually loading env for this test script to avoid dependency if dotenv fails
# But since we installed it, we can use it.
from dotenv import load_dotenv

load_dotenv()

proxy_url = os.getenv("PROXY_URL")
if not proxy_url:
    # Fallback to hardcoded string if reading .env fails (for test purposes only)
    proxy_url = "http://oxsjenoi-residential-rota:yw72fdfu37vt@p.webshare.io:80"

print(f"Testing Proxy: {proxy_url.split('@')[1]}") # Print only host part for privacy in logs

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
    print("(Note: This IP should be different from your local IP because of the proxy)")
    
except Exception as e:
    print(f"Proxy Connection Failed: {e}")
ÿ"(ca1727485638ccaf6b3e68d8324b9fc1980f23af27file:///c:/Users/Yorri/.gemini/test_proxy_connection.py:file:///c:/Users/Yorri/.gemini