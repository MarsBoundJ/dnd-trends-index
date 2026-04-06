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

# Proxy routing — GOST bridge runs on localhost:3128 (see entrypoint.sh)
PROXY_LOCAL = {"server": "http://localhost:3128"}

# Safety caps — prevent runaway proxy bandwidth burns
MAX_CONSECUTIVE_FAILURES = 3   # Stop if this many terms in a row yield no data
MAX_RUNTIME_MINUTES = 45       # Hard wall-clock cap per job execution

async def human_mimicry(page):
    """Task 3: Implement 'Human-Like' Interaction."""
    print("  [*] Performing human mimicry (Jiggle & Scroll)...")
    
    # Random sleep 1-4s
    await asyncio.sleep(random.uniform(1, 4))
    
    # Gentle Page Scroll
    await page.mouse.wheel(0, random.randint(200, 500))
    await asyncio.sleep(random.uniform(0.5, 1.5))
    
    # Random Mouse Jiggle
    for _ in range(3):
        x, y = random.randint(100, 500), random.randint(100, 500)
        await page.mouse.move(x, y, steps=10)
        await asyncio.sleep(random.uniform(0.2, 0.5))

async def scrape_with_retry(term_info, retries=1):
    term = term_info['search_term']
    
    # Phase 52c Local Stealth Bridge
    proxy_options = [PROXY_LOCAL]
    
    for proxy_cfg in proxy_options:
        proxy_type = "GOST_BRIDGE"
        print(f"\n[SCAN] Scrying for: {term} via {proxy_type}")
        
        try:
            encoded_term = term.replace(" ", "%20").replace("'", "%27")
            url = f"https://trends.google.com/trends/explore?q={encoded_term}&date=2026-02-01%202026-03-01&geo=US"
            
            async with async_playwright() as p:
                cookies_to_add = []
                # Removed curl_cffi pre-fetch logic

                browser = await p.firefox.launch(
                    headless=True,
                    proxy=proxy_cfg
                )
                
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    viewport={'width': 1280, 'height': 720},
                    ignore_https_errors=True
                )
                
                page = await context.new_page()
                
                # Task 2: Inject Playwright Stealth
                await Stealth().apply_stealth_async(page)
                
                # Task 2: Session Priming
                print(f"  [*] Priming Session: Navigating to Google Trends Homepage...")
                prime_response = await page.goto("https://trends.google.com/trends/", wait_until="load", timeout=90000)
                if prime_response:
                    print(f"  [HTTP] Priming Page Status: {prime_response.status}")
                await human_mimicry(page)
                print(f"  [*] Priming Complete. Proceeding to target keyword...")

                captured_data = []

                async def handle_response(response):
                    # Log all Trends-related responses
                    if "trends.google.com" in response.url:
                        pass # too noisy
                    
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
                        except Exception: pass

                page.on("response", handle_response)
                
                print(f"  [*] Navigating to keyword URL...")
                response = await page.goto(url, wait_until="load", timeout=90000)
                
                if response:
                    print(f"  [HTTP] Main Page Status: {response.status}")
                    if response.status != 200:
                        print(f"  [!] Navigation Error Code: {response.status}")
                        # For Checkpoint: Capture Title/HTML on failure
                        title = await page.title()
                        html = await page.content()
                        print(f"  [DEBUG] Page Title: {title}")
                        print(f"  [DEBUG] HTML Body (Snipped):\n{html[:1000]}")
                
                # Task 3: Human Interactions - Part 1
                await human_mimicry(page)
                
                # Enhanced Interception Wait
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
                
                await browser.close()
                return captured_data
                
        except Exception as e:
            print(f"  [!] Browser Error ({proxy_type}): {e}")
            
    return []

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
        # Create a mock term_row-like structure for the manual override
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

    start_time = datetime.datetime.now()
    consecutive_failures = 0
    terms_processed = 0

    for term_row in terms:
        # Hard runtime cap
        elapsed_minutes = (datetime.datetime.now() - start_time).total_seconds() / 60
        if elapsed_minutes > MAX_RUNTIME_MINUTES:
            print(f"[!] Runtime cap reached ({MAX_RUNTIME_MINUTES}m after {terms_processed} terms). Stopping cleanly.")
            break

        term_dict = {'term_id': term_row.term_id, 'search_term': term_row.search_term}
        data = await scrape_with_retry(term_dict)
        terms_processed += 1

        if data:
            consecutive_failures = 0
            rows = []
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

            errors = client.insert_rows_json(DEST_TABLE, rows)
            if not errors:
                print(f"  [BQ] Inserted {len(rows)} rows for {term_row.search_term}.")
            else:
                print(f"  [BQ] Errors inserting {term_row.search_term}: {errors}")
        else:
            consecutive_failures += 1
            print(f"  [-] No data for {term_row.search_term}. Consecutive failures: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"[!] {MAX_CONSECUTIVE_FAILURES} consecutive failures — proxy may be dead. Stopping to protect bandwidth.")
                sys.exit(1)

        await asyncio.sleep(random.uniform(2, 5))

    print(f"[*] Done. Processed {terms_processed} terms in {(datetime.datetime.now() - start_time).total_seconds() / 60:.1f}m.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--keyword", type=str, default=None, help="Specific keyword to scry (e.g. 'Monk')")
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit, keyword=args.keyword))
