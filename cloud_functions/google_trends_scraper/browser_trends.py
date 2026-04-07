import asyncio
import json
import datetime
import random
import uuid
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from google.cloud import bigquery
import os
import sys

# Constants
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
DEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"

SOURCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"

def fetch_terms_to_process(client, limit=100):
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    query = f"""
        SELECT e.term_id, e.search_term, t.last_date
        FROM `{SOURCE_TABLE}` e
        LEFT JOIN (
            SELECT search_term, MAX(date) as last_date
            FROM `{DEST_TABLE}`
            GROUP BY search_term
        ) t ON e.search_term = t.search_term
        WHERE e.is_pilot = TRUE
          AND (t.last_date IS NULL OR t.last_date < '{yesterday}')
        LIMIT {limit}
    """
    return list(client.query(query).result())

# Proxy — read from env var set by Cloud Run job
# Format: http://user:pass@host:port
_PROXY_URL_ENV = os.environ.get("PROXY_URL", "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80")

def _parse_proxy(url: str) -> dict:
    """Parse http://user:pass@host:port into Playwright proxy dict with explicit username/password fields."""
    from urllib.parse import urlparse
    p = urlparse(url)
    proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        proxy["username"] = p.username
    if p.password:
        proxy["password"] = p.password
    return proxy

PROXY_LOCAL = _parse_proxy(_PROXY_URL_ENV)

# Safety cap — prevent runaway runs from burning proxy bandwidth
MAX_RUNTIME_MINUTES = 45

async def human_mimicry(page):
    """Human-like interaction: random sleep, scroll, mouse jiggle."""
    print("  [*] Performing human mimicry (Jiggle & Scroll)...")
    await asyncio.sleep(random.uniform(1, 4))
    await page.mouse.wheel(0, random.randint(200, 500))
    await asyncio.sleep(random.uniform(0.5, 1.5))
    for _ in range(3):
        x, y = random.randint(100, 500), random.randint(100, 500)
        await page.mouse.move(x, y, steps=10)
        await asyncio.sleep(random.uniform(0.2, 0.5))

async def scrape_term(page, term_info):
    """
    Scrape a single term using a shared, already-warm browser page.
    Returns:
      list[dict]  — data points (may be [] for genuine zero-interest)
      None        — page/navigation error
    """
    term = term_info['search_term']
    encoded_term = term.replace(" ", "%20").replace("'", "%27")
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    start = yesterday - datetime.timedelta(days=365)
    url = f"https://trends.google.com/trends/explore?q={encoded_term}&date={start.isoformat()}%20{yesterday.isoformat()}&geo=US"

    captured_data = []

    def handle_response(response):
        if "trends.google.com/trends/api/widgetdata/multiline" in response.url:
            print(f"    [INTERCEPT] {response.status} | {response.url[:100]}...")

    async def handle_response_async(response):
        if "trends.google.com/trends/api/widgetdata/multiline" in response.url:
            print(f"    [INTERCEPT] {response.status} | {response.url[:100]}...")
            try:
                if response.status == 200:
                    text = await response.text()
                    if text.startswith(")]}'"): text = text[5:]
                    data = json.loads(text)
                    timeline = data.get("default", {}).get("timelineData", [])
                    for entry in timeline:
                        dt = datetime.datetime.fromtimestamp(int(entry["time"]))
                        captured_data.append({
                            "date": dt.date().isoformat(),
                            "interest": entry["value"][0],
                            "is_partial": bool(entry.get("isPartial", False))
                        })
            except Exception:
                pass

    page.on("response", handle_response_async)

    try:
        print(f"  [*] Navigating to keyword URL...")
        response = await page.goto(url, wait_until="load", timeout=90000)

        if response:
            print(f"  [HTTP] Main Page Status: {response.status}")
            if response.status != 200:
                print(f"  [!] Navigation Error Code: {response.status}")
                title = await page.title()
                html = await page.content()
                print(f"  [DEBUG] Page Title: {title}")
                print(f"  [DEBUG] HTML Body (Snipped):\n{html[:1000]}")

        await human_mimicry(page)

        print("  [*] Waiting up to 45s for chart and API pulse...")
        for i in range(45):
            if captured_data:
                print("  [+] Data intercepted! Proceeding...")
                break
            await asyncio.sleep(1)
            if i == 10 and not captured_data:
                print("  [*] 10s elapsed with no data. Triggering gentle scroll...")
                await page.mouse.wheel(0, random.randint(200, 500))

        if not captured_data:
            print("  [-] No data points intercepted. Saving debug HTML...")
            html = await page.content()
            with open("debug_scry.html", "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  [DEBUG] Page Title: {await page.title()}")

        page.remove_listener("response", handle_response_async)
        return captured_data

    except Exception as e:
        print(f"  [!] Page Error: {e}")
        page.remove_listener("response", handle_response_async)
        return None


async def check_proxy_health():
    """Quick proxy health check before processing any terms. Fail fast if dead."""
    import urllib.request
    proxy_handler = urllib.request.ProxyHandler({"https": "http://localhost:3128"})
    opener = urllib.request.build_opener(proxy_handler)
    try:
        opener.open("https://trends.google.com/trends/", timeout=15)
        print("[*] Proxy health check passed.")
        return True
    except Exception as e:
        print(f"[!] Proxy health check FAILED: {e}")
        print("[!] Aborting — proxy is not available. No bandwidth will be wasted.")
        return False

async def main(limit=50, keyword=None):
    client = bigquery.Client(project=PROJECT_ID)
    batch_id = "GHOST_WALK_" + str(uuid.uuid4())[:8]
    fetched_at = datetime.datetime.now().isoformat()

    if keyword:
        class MockTermRow:
            def __init__(self, t):
                self.term_id = f"manual_{t}"
                self.search_term = t
        terms = [MockTermRow(keyword)]
        print(f"[*] Running targeted smoke test for: {keyword}")
    else:
        # Proxy health check — abort immediately if proxy is dead
        if not await check_proxy_health():
            sys.exit(1)

        terms = fetch_terms_to_process(client, limit=limit)
        print(f"[*] Fetched {len(terms)} terms to process.")

    if not terms:
        print("[*] No stale terms found. Exiting.")
        return

    MAX_CONSECUTIVE_FAILURES = 3
    consecutive_failures = 0
    start_time = datetime.datetime.now()

    print("[*] Starting browser session (single session for entire batch)...")
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True, proxy=PROXY_LOCAL)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            viewport={'width': 1280, 'height': 720},
            ignore_https_errors=True
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        # Prime the session once — warm up with the homepage so subsequent term
        # requests don't hit Google's cold-start 429 rate limit.
        print("[*] Priming session: navigating to Google Trends homepage...")
        prime_response = await page.goto("https://trends.google.com/trends/", wait_until="load", timeout=90000)
        if prime_response:
            print(f"  [HTTP] Priming Page Status: {prime_response.status}")
        await human_mimicry(page)
        print("[*] Priming complete. Beginning term scrape...")

        for term_row in terms:
            elapsed_minutes = (datetime.datetime.now() - start_time).total_seconds() / 60
            if elapsed_minutes > MAX_RUNTIME_MINUTES:
                print(f"[!] Runtime cap reached ({MAX_RUNTIME_MINUTES}m). Stopping cleanly.")
                break

            term_dict = {'term_id': term_row.term_id, 'search_term': term_row.search_term}
            print(f"\n[SCAN] Scrying for: {term_row.search_term}")
            data = await scrape_term(page, term_dict)

            if data is None:
                consecutive_failures += 1
                print(f"  [-] Page error for {term_row.search_term}. Consecutive failures: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}")
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"  [!] {MAX_CONSECUTIVE_FAILURES} consecutive failures — proxy may be down. Stopping.")
                    await browser.close()
                    sys.exit(1)
            else:
                consecutive_failures = 0
                rows = []

                if data:
                    for d in data:
                        rows.append({
                            "term_id": term_row.term_id,
                            "search_term": term_row.search_term,
                            "date": d["date"],
                            "interest": int(d["interest"]),
                            "is_partial": d["is_partial"],
                            "fetched_at": fetched_at,
                            "batch_id": batch_id
                        })
                else:
                    # Zero-interest confirmed by API — insert a zero row so gap filter advances
                    print(f"  [~] Zero interest confirmed for {term_row.search_term} — recording zero.")
                    rows.append({
                        "term_id": term_row.term_id,
                        "search_term": term_row.search_term,
                        "date": datetime.date.today().isoformat(),
                        "interest": 0,
                        "is_partial": False,
                        "fetched_at": fetched_at,
                        "batch_id": batch_id
                    })

                errors = client.insert_rows_json(DEST_TABLE, rows)
                if not errors:
                    print(f"  [BQ] Inserted {len(rows)} rows for {term_row.search_term}.")
                else:
                    print(f"  [BQ] Errors inserting {term_row.search_term}: {errors}")

            await asyncio.sleep(random.uniform(2, 5))

        await browser.close()
        print(f"\n[*] Batch complete. Session closed.")


    print(f"[*] Done. Processed {terms_processed} terms in {(datetime.datetime.now() - start_time).total_seconds() / 60:.1f}m.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--keyword", type=str, default=None, help="Specific keyword to scry (e.g. 'Monk')")
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit, keyword=args.keyword))
