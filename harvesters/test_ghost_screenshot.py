import os
import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from urllib.parse import urlparse
_p = urlparse(os.environ.get("PROXY_URL", ""))
PROXY = {"server": f"http://{_p.hostname}:{_p.port or 80}", "username": _p.username, "password": _p.password}

async def test_ghost_walk():
    print("Testing Ghost Walk Navigation...")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                proxy=PROXY
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 720},
                ignore_https_errors=True
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            print("  [*] Navigating to Google Trends...")
            response = await page.goto("https://trends.google.com/trends/explore?q=Fighter", wait_until="commit", timeout=30000)
            print(f"  [-] Status: {response.status if response else 'None'}")
            
            print("  [*] Waiting 5s for any dynamic checks...")
            await asyncio.sleep(5)
            
            print("  [*] Capturing screenshot and HTML...")
            await page.screenshot(path="/app/ghost_debug.png")
            
            html = await page.content()
            with open("/app/ghost_debug.html", "w", encoding="utf-8") as f:
                f.write(html)
                
            print("  [+] Captured to /app/ghost_debug.png and /app/ghost_debug.html")
            await browser.close()
    except Exception as e:
        print(f"  [!] Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_ghost_walk())
