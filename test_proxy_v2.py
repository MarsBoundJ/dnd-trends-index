import requests
import os

proxy_url = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"
proxies = {
    "http": proxy_url,
    "https": proxy_url
}

print(f"Testing proxy: {proxy_url[:20]}...")

try:
    # Test with a simple IP check
    response = requests.get("http://ifconfig.me", proxies=proxies, timeout=10)
    print(f"HTTP Status: {response.status_code}")
    print(f"Detected IP: {response.text.strip()}")
    
    response = requests.get("https://ifconfig.me", proxies=proxies, timeout=10)
    print(f"HTTPS Status: {response.status_code}")
    print(f"Detected IP: {response.text.strip()}")
    
except Exception as e:
    print(f"Proxy test failed: {e}")
