import asyncio
from playwright.async_api import async_playwright
import os

PROXY_URL = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

async def dump_source(term):
    print(f"[*] Dumping Source for: {term}")
    proxy_part = PROXY_URL.replace("http://", "")
    auth_part, host_part = proxy_part.split("@")
    p_user, p_pass = auth_part.split(":")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, proxy={"server": f"http://{host_part}", "username": p_user, "password": p_pass})
        page = await browser.new_page()
        url = f"https://trends.google.com/trends/explore?q={term.replace(' ', '%20')}&date=2026-02-01%202026-03-01&geo=US"
        
        try:
            # wait_until='commit' is very fast, just waits for response
            await page.goto(url, wait_until="commit", timeout=120000)
            await asyncio.sleep(10)
            content = await page.content()
            with open("trends_dump.html", "w") as f:
                f.write(content)
            print("[*] Dumped to trends_dump.html")
        except Exception as e:
            print(f"[!] Error: {e}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(dump_source("Daggerheart"))
