import os
import feedparser
import cloudscraper

PROXY_URL = os.environ.get("PROXY_URL")
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}

url = "https://itch.io/jams.xml"
print(f"Fetching {url}")

scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
response = scraper.get(url, proxies=PROXIES, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True, timeout=30)
print(f"Status: {response.status_code}")

feed = feedparser.parse(response.content)
print(f"Jams parsed: {len(feed.entries)}")
for i, e in enumerate(feed.entries[:3]):
    print(i, e.get('title', ''))
    print(e.get('summary', '')[:200])
