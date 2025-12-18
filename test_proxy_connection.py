
import requests
import os

# Hardcoded static user for debugging
# Username from Screenshot 4: oxsjenoi-residential-1
# Password from Screenshot 4: yw72fdfu37vt
proxy_url = "http://oxsjenoi-residential-1:yw72fdfu37vt@p.webshare.io:80"

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
