import cloudscraper

PROXY_URL = "http://lcbaurkt-US-rotate:q8aa993piq8h@p.webshare.io:80"
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}

scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
html = scraper.get('https://itch.io/jams?filter=physical', proxies=PROXIES).text

print(html[:5000])
with open('/tmp/jams_html.txt', 'w', encoding='utf-8') as f:
    f.write(html)
