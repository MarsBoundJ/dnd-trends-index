import requests
import time

proxy_url = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"
proxies = {
    "http": proxy_url,
    "https": proxy_url
}

print("Checking IP rotation (5 attempts)...")
ips = set()
for i in range(5):
    try:
        r = requests.get("https://ifconfig.me", proxies=proxies, timeout=10)
        ip = r.text.strip()
        ips.add(ip)
        print(f"Attempt {i+1}: {ip}")
    except Exception as e:
        print(f"Attempt {i+1} failed: {e}")
    time.sleep(2)

print(f"\nUnique IPs found: {len(ips)}")
