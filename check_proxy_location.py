
import requests
import time

PROXY_URL = "http://p.webshare.io:9999"

proxies = {
    "http": PROXY_URL,
    "https": PROXY_URL,
}

print("Sampling Proxy Locations (10 samples)...")

def get_location():
    try:
        # ip-api.com returns JSON with country, region, city
        response = requests.get("http://ip-api.com/json", proxies=proxies, timeout=10)
        data = response.json()
        return f"{data.get('country')} ({data.get('countryCode')})"
    except Exception as e:
        return f"Error: {str(e)[:50]}"

for i in range(10):
    loc = get_location()
    print(f"Sample {i+1}: {loc}")
    time.sleep(1) # wait a bit for rotation
