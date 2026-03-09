import asyncio
from playwright.async_api import async_playwright
import os

PROXY_URL = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

async def debug_scrape(term):
    print(f"[*] Debugging: {term}")
    proxy_part = PROXY_URL.replace("http://", "")
    auth_part, host_part = proxy_part.split("@")
    p_user, p_pass = auth_part.split(":")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy={
                "server": f"http://{host_part}",
                "username": p_user,
                "password": p_pass
            }
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ignore_https_errors=True
        )
        page = await context.new_page()
        
        url = f"https://trends.google.com/trends/explore?q={term.replace(' ', '%20')}&date=2026-02-01%202026-03-01&geo=US"
        print(f"[*] Navigating to: {url} (Timeout 120s)")
        
        try:
            # Use load or domcontentloaded to be less picky
            await page.goto(url, wait_until="load", timeout=120000)
            print("[*] Page loaded. Waiting 15s for dynamic content...")
            await asyncio.sleep(15)
        except Exception as e:
            print(f"[!] Navigation error or timeout: {e}")
            
        finally:
            print("[*] Taking final scrying screenshot...")
            await page.screenshot(path="final_debug.png", full_page=True)
            content = await page.content()
            with open("final_debug.html", "w") as f:
                f.write(content)
            print("[*] Scrying materials saved to final_debug.png/html")
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_scrape("Daggerheart"))
