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
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        url = f"https://trends.google.com/trends/explore?q={term.replace(' ', '%20')}&date=2026-02-01%202026-03-01&geo=US"
        print(f"[*] Navigating to: {url}")
        
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(5)
            await page.screenshot(path="debug_screenshot.png")
            print("[*] Screenshot saved to debug_screenshot.png")
            
            content = await page.content()
            with open("debug_page.html", "w") as f:
                f.write(content)
            print("[*] Page source saved to debug_page.html")
            
        except Exception as e:
            print(f"[!] Error: {e}")
            await page.screenshot(path="debug_error.png")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_scrape("Daggerheart"))
