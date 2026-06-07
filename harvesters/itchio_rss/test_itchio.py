import os
import cloudscraper
import requests

PROXY_URL = os.environ.get("PROXY_URL")
PROXIES = {
    "http": PROXY_URL,
    "https": PROXY_URL
}
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

url = "https://itch.io/games/tag-tabletop-rpg.xml"

print("1. Testing standard requests...")
try:
    session = requests.Session()
    session.proxies.update(PROXIES)
    r1 = session.get(url, headers=headers, allow_redirects=True, timeout=15)
    print(f"Status: {r1.status_code}")
    print(f"Length: {len(r1.content)}")
except Exception as e:
    print(f"Error: {e}")

print("\n2. Testing cloudscraper...")
try:
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    r2 = scraper.get(url, proxies=PROXIES, headers=headers, allow_redirects=True, timeout=30)
    print(f"Status: {r2.status_code}")
    print(f"Length: {len(r2.content)}")
    print(r2.content[:200])
except Exception as e:
    print(f"Error: {e}")
