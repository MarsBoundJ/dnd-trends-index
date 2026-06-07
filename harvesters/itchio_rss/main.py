import feedparser
import datetime
import urllib.parse
import re
import json
from google.cloud import bigquery
import os
import time
import requests
import cloudscraper
from bs4 import BeautifulSoup

PROJECT_ID = "dnd-trends-index"
PROXY_URL = os.getenv("PROXY_URL")
PROXIES = {
    "http": PROXY_URL,
    "https": PROXY_URL
}

# ─────────────────────────────────────────────
# Shared fetch helpers
# ─────────────────────────────────────────────

def fetch_direct(url, headers=None, timeout=15):
    """Plain requests fetch — no proxy."""
    headers = headers or {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    response = requests.get(url, headers=headers, allow_redirects=True, timeout=timeout)
    return response

def fetch_via_proxy(url, headers=None, timeout=30):
    """Cloudscraper fetch through Webshare rotating proxy."""
    headers = headers or {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    return scraper.get(url, proxies=PROXIES, headers=headers, allow_redirects=True, timeout=timeout)

def fetch_content(url):
    """Try direct first (no proxy cost), fall back to proxy if needed."""
    print(f"Fetching {url} directly...")
    try:
        r = fetch_direct(url)
        if r.status_code == 200 and r.content.strip():
            return r.content
        print(f"Direct returned {r.status_code} — trying proxy...")
    except Exception as e:
        print(f"Direct failed: {e} — trying proxy...")

    print(f"Fetching {url} via proxy...")
    try:
        r = fetch_via_proxy(url)
        if r.status_code == 200 and r.content.strip():
            return r.content
        print(f"Proxy returned {r.status_code} — giving up.")
    except Exception as e:
        print(f"Proxy failed: {e}")

    return None

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def fetch_existing_ids(client, table_ref, id_column):
    """Return set of existing IDs to avoid duplicate inserts."""
    try:
        rows = client.query(f"SELECT {id_column} FROM `{table_ref}`").result()
        return set(row[id_column] for row in rows)
    except Exception as e:
        print(f"Could not fetch existing IDs from {table_ref}: {e}")
        return set()

# ─────────────────────────────────────────────
# Products (RSS — latest tabletop RPG releases)
# ─────────────────────────────────────────────

def fetch_and_insert_products(client):
    """Fetch latest tabletop RPG games from itch.io RSS and insert new ones."""
    url = "https://itch.io/games/tag-tabletop-rpg.xml"
    table_ref = f"{PROJECT_ID}.dnd_trends_raw.itchio_products"

    content = fetch_content(url)
    if not content:
        print("Could not fetch products feed.")
        return 0

    feed = feedparser.parse(content)
    if not feed.entries:
        print("No entries in products feed.")
        return 0

    collected_date = datetime.datetime.utcnow().isoformat()
    existing_ids = fetch_existing_ids(client, table_ref, "product_id")

    new_rows = []
    for entry in feed.entries:
        link = entry.get('link', '')
        parsed_url = urllib.parse.urlparse(link)
        product_id = parsed_url.netloc.split('.')[0] + parsed_url.path if parsed_url.netloc else link

        if product_id in existing_ids:
            continue

        raw_tags = entry.get('tags', [])
        tags = [t.get('term', '') for t in raw_tags if t.get('term')]

        new_rows.append({
            "product_id": product_id,
            "title": entry.get('title', ''),
            "creator": entry.get('author', ''),
            "price": 0.0,
            "is_pwyw": False,
            "tags": tags,
            "aesthetic_clusters": [],
            "list_type": "tag-tabletop-rpg",
            "collected_date": collected_date,
        })

    if not new_rows:
        print(f"No new products ({len(feed.entries)} already known).")
        return 0

    errors = client.insert_rows_json(table_ref, new_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for products: {errors}")

    print(f"Inserted {len(new_rows)} new products ({len(feed.entries) - len(new_rows)} already known).")
    return len(new_rows)

# ─────────────────────────────────────────────
# Jams (HTML scrape — itch.io has no jams RSS)
# ─────────────────────────────────────────────

def fetch_jams_html():
    """Scrape itch.io jams page — physical/tabletop filter."""
    url = "https://itch.io/jams?filter=physical"
    print(f"Fetching jams HTML from {url}...")

    # Jams page requires JS rendering — proxy first for reliability
    try:
        r = fetch_via_proxy(url)
        if r.status_code == 200 and r.text.strip():
            return r.text
        print(f"Proxy returned {r.status_code} for jams, trying direct...")
    except Exception as e:
        print(f"Proxy failed for jams: {e}, trying direct...")

    try:
        r = fetch_direct(url)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"Direct failed for jams: {e}")

    return None

def extract_jams(html_content):
    """Parse jams from itch.io React JSON embedded in page."""
    rows = []
    script_match = re.search(
        r'R\.Jam\.FilteredJamCalendar\(\{"jams":(\[.*?\])\}\)', html_content
    )
    if script_match:
        try:
            jams_data = json.loads(script_match.group(1))
            for j in jams_data:
                rows.append({
                    "jam_id":          str(j.get('id', '')),
                    "jam_title":       j.get('title', 'Unknown Jam'),
                    "theme_keywords":  [],
                    "submission_count": j.get('joined', 0),
                    "start_date":      j.get('start_date') or None,
                    "end_date":        j.get('end_date') or None,
                })
        except json.JSONDecodeError as e:
            print(f"Failed to decode jams JSON: {e}")
    else:
        print("Could not find jam data in page HTML.")
    return rows

def fetch_and_insert_jams(client):
    """Fetch itch.io jams and insert new ones into BigQuery."""
    table_ref = f"{PROJECT_ID}.dnd_trends_raw.itchio_jams"

    html = fetch_jams_html()
    if not html:
        print("Could not fetch jams page.")
        return 0

    jams = extract_jams(html)
    if not jams:
        print("No jams extracted from page.")
        return 0

    print(f"Extracted {len(jams)} jams from HTML.")

    existing_ids = fetch_existing_ids(client, table_ref, "jam_id")
    new_rows = [j for j in jams if j["jam_id"] not in existing_ids]

    if not new_rows:
        print(f"No new jams ({len(jams)} already known).")
        return 0

    errors = client.insert_rows_json(table_ref, new_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for jams: {errors}")

    print(f"Inserted {len(new_rows)} new jams ({len(jams) - len(new_rows)} already known).")
    return len(new_rows)

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    client = get_bq_client()
    total_products = 0
    total_jams = 0

    # 1. Latest tabletop RPG products (RSS)
    try:
        total_products += fetch_and_insert_products(client)
    except Exception as e:
        print(f"Error processing products: {e}")

    time.sleep(2)

    # 2. Physical/tabletop jams (HTML scrape)
    try:
        total_jams += fetch_and_insert_jams(client)
    except Exception as e:
        print(f"Error processing jams: {e}")

    print(f"\nDone. {total_products} new products, {total_jams} new jams inserted.")
