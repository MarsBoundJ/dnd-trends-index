import requests
import os

proxy_url = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"
proxies = {
    "http": proxy_url,
    "https": proxy_url
}

print(f"Testing proxy access to Google Trends...")

try:
    url = "https://trends.google.com/trends/api/explore?hl=en-US&tz=360"
    response = requests.get(url, proxies=proxies, timeout=15)
    print(f"Status: {response.status_code}")
    print(f"Text Snippet: {response.text[:100]}")
    
except Exception as e:
    print(f"FAILED: {e}")
