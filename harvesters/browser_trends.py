import asyncio
import json
import datetime
import random
import uuid
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from curl_cffi import requests as c_requests
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

# Task 1: The SOCKS5 Pivot and Protocol Hardening to Port 9999
PROXY_SOCKS = {"server": "socks5://p.webshare.io:9999", "username": "lcbaurkt-US-rotate", "password": "q8aa993piq8h"}
PROXY_HTTP = {"server": "http://p.webshare.io:80", "username": "lcbaurkt-US-rotate", "password": "q8aa993piq8h"}

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
    
    # Phase 51.1 - Using SOCKS5 port 9999
    proxy_options = [
        PROXY_SOCKS,
        PROXY_HTTP
    ]
    
    for proxy_cfg in proxy_options:
        proxy_type = "SOCKS5" if "socks5" in proxy_cfg["server"] else "HTTP"
        print(f"\n[SCAN] Scrying for: {term} via {proxy_type}")
        
        try:
            encoded_term = term.replace(" ", "%20").replace("'", "%27")
            url = f"https://trends.google.com/trends/explore?q={encoded_term}&date=2026-02-01%202026-03-01&geo=US"
            
            async with async_playwright() as p:
                cookies_to_add = []
                print("  [*] Applying curl_cffi TLS mimicry to pre-fetch cookies...")
                try:
                    proxy_url_cffi = proxy_cfg["server"]
                    auth = f"{proxy_cfg['username']}:{proxy_cfg['password']}@"
                    if "socks" in proxy_url_cffi:
                        c_proxy = proxy_url_cffi.replace("socks5://", f"socks5://{auth}")
                    else:
                        c_proxy = proxy_url_cffi.replace("http://", f"http://{auth}")

                    c_session = c_requests.Session(
                        impersonate="firefox",
                        proxies={"http": c_proxy, "https": c_proxy}
                    )
                    c_res = c_session.get(url, timeout=30)
                    print(f"  [curl_cffi] Status: {c_res.status_code}")
                    for name, value in c_session.cookies.get_dict().items():
                        cookies_to_add.append({
                            "name": name,
                            "value": value,
                            "domain": ".google.com",
                            "path": "/",
                            "secure": True,
                            "httpOnly": False
                        })
                except Exception as ce:
                    print(f"  [curl_cffi Error] {ce}")

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
                if cookies_to_add:
                    await context.add_cookies(cookies_to_add)
                    print(f"  [+] Injected {len(cookies_to_add)} curl_cffi cookies.")
                
                # Task 2: Inject Playwright Stealth
                await Stealth().apply_stealth_async(page)
                
                captured_data = []

                async def handle_response(response):
                    # Log all Trends-related responses
                    if "trends.google.com" in response.url:
                        print(f"    [INTERCEPT] {response.status} | {response.url[:100]}...")
                    
                    if "trends.google.com/trends/api/widgetdata/multiline" in response.url:
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
                
                print(f"  [*] Navigating to Trends via Firefox Engine...")
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
                
                # Task 3: Human Interactions
                await human_mimicry(page)
                
                print("  [*] Waiting 20s for chart and API pulse...")
                await asyncio.sleep(20)
                
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

async def main(limit=100):
    client = bigquery.Client(project=PROJECT_ID)
    batch_id = "GHOST_WALK_" + str(uuid.uuid4())[:8]
    fetched_at = datetime.datetime.now().isoformat()
    
    terms = fetch_terms_to_process(client, limit=limit)
    print(f"[*] Fetched {len(terms)} terms to process.")
    
    for term_row in terms:
        term_dict = {'term_id': term_row.term_id, 'search_term': term_row.search_term}
        data = await scrape_with_retry(term_dict)
        
        if data:
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

        await asyncio.sleep(random.uniform(2, 5))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit))
