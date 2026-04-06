import feedparser
import datetime
import urllib.parse
from google.cloud import bigquery
import os
import time
import requests
import cloudscraper

PROJECT_ID = "dnd-trends-index"
PROXY_URL = os.getenv("PROXY_URL", "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80")
PROXIES = {
    "http": PROXY_URL,
    "https": PROXY_URL
}

def fetch_feed_content(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    # Try direct first — itch.io RSS feeds are public, no proxy needed
    print(f"Fetching {url} directly...")
    try:
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=15)
        if response.status_code == 200 and response.content.strip():
            return response.content
        print(f"Direct request returned {response.status_code}, trying proxy...")
    except Exception as e:
        print(f"Direct request failed: {e}, trying proxy...")

    # Proxy fallback
    print(f"Fetching {url} via proxy...")
    session = requests.Session()
    session.proxies.update(PROXIES)
    try:
        response = session.get(url, headers=headers, allow_redirects=True, timeout=15)
        if response.status_code == 200 and response.content.strip():
            return response.content
    except Exception as e:
        print(f"Proxy request failed: {e}, trying Cloudscraper...")

    print("Falling back to Cloudscraper...")
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    response = scraper.get(url, proxies=PROXIES, headers=headers, allow_redirects=True, timeout=30)
    return response.content

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def fetch_existing_ids(client, table_ref, id_column):
    """Fetch all existing IDs from a table to avoid duplicate inserts."""
    try:
        rows = client.query(f"SELECT {id_column} FROM `{table_ref}`").result()
        return set(row[id_column] for row in rows)
    except Exception as e:
        print(f"Could not fetch existing IDs from {table_ref}: {e}")
        return set()

def fetch_and_merge_products(client, feed_url, list_type):
    print(f"Fetching products for ({list_type})...")
    content = fetch_feed_content(feed_url)
    feed = feedparser.parse(content)

    if not feed.entries:
        print(f"No entries found for {list_type}.")
        return 0

    collected_date = datetime.datetime.utcnow().isoformat()
    table_ref = f"{PROJECT_ID}.dnd_trends_raw.itchio_products"

    rows = []
    for entry in feed.entries:
        link = entry.get('link', '')
        parsed_url = urllib.parse.urlparse(link)
        product_id = parsed_url.netloc.split('.')[0] + parsed_url.path if parsed_url.netloc else link

        # Extract tags as plain strings from entry
        raw_tags = entry.get('tags', [])
        tags = [t.get('term', '') for t in raw_tags if t.get('term')]

        rows.append({
            "product_id": product_id,
            "title": entry.get('title', ''),
            "creator": entry.get('author', ''),
            "price": 0.0,
            "is_pwyw": False,
            "tags": tags,
            "aesthetic_clusters": [],
            "list_type": list_type,
            "collected_date": collected_date,
        })

    # Only insert products we haven't seen before
    existing_ids = fetch_existing_ids(client, table_ref, "product_id")
    new_rows = [r for r in rows if r["product_id"] not in existing_ids]

    if not new_rows:
        print(f"No new products for {list_type} ({len(rows)} already known).")
        return 0

    errors = client.insert_rows_json(table_ref, new_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for {list_type}: {errors}")

    print(f"Inserted {len(new_rows)} new products for {list_type} ({len(rows) - len(new_rows)} already known).")
    return len(new_rows)

def fetch_and_merge_jams(client, feed_url):
    print(f"Fetching jams from {feed_url}...")
    content = fetch_feed_content(feed_url)
    feed = feedparser.parse(content)

    if not feed.entries:
        print("No jams found.")
        return 0

    table_ref = f"{PROJECT_ID}.dnd_trends_raw.itchio_jams"
    existing_ids = fetch_existing_ids(client, table_ref, "jam_id")

    rows = []
    for entry in feed.entries:
        link = entry.get('link', '')
        parsed_url = urllib.parse.urlparse(link)
        jam_id = parsed_url.path

        if jam_id in existing_ids:
            continue

        start_date = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            start_date = datetime.datetime(*entry.published_parsed[:6]).isoformat()

        rows.append({
            "jam_id": jam_id,
            "jam_title": entry.get('title', ''),
            "theme_keywords": [],
            "submission_count": 0,
            "start_date": start_date,
            "end_date": None,
        })

    if not rows:
        print("No new jams to insert.")
        return 0

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for jams: {errors}")

    print(f"Inserted {len(rows)} new jams.")
    return len(rows)

if __name__ == "__main__":
    client = get_bq_client()
    total_products = 0
    total_jams = 0

    # 1. Product Feeds
    product_feeds = [
        ("https://itch.io/games/tag-tabletop-rpg.xml", "tag-tabletop-rpg"),
        ("https://itch.io/games/tag-tabletop-rpg/top-sellers.xml", "top_sellers"),
        ("https://itch.io/games/tag-tabletop-rpg/new-and-popular.xml", "new_popular"),
    ]

    for url, l_type in product_feeds:
        try:
            total_products += fetch_and_merge_products(client, url, l_type)
        except Exception as e:
            print(f"Error processing {l_type}: {e}")
            if hasattr(e, 'errors'):
                print(f"Details: {e.errors}")
        time.sleep(2)

    # 2. Jams Feed
    try:
        total_jams += fetch_and_merge_jams(client, "https://itch.io/jams/upcoming.xml")
    except Exception as e:
        print(f"Error processing jams: {e}")
        if hasattr(e, 'errors'):
            print(f"Details: {e.errors}")

    print(f"\nDone. {total_products} new products, {total_jams} new jams inserted.")
