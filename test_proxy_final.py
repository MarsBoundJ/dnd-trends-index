import requests
import os

proxy = os.environ.get("PROXY_URL")
print(f"Testing Proxy: {proxy}")

try:
    proxies = {"http": proxy, "https": proxy}
    r = requests.get("https://ipv4.icanhazip.com", proxies=proxies, timeout=10)
    print(f"Success! Proxy IP: {r.text.strip()}")
except Exception as e:
    print(f"Proxy Test Failed: {e}")
