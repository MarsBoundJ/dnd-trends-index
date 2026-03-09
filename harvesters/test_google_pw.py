import asyncio
from playwright.async_api import async_playwright
import os

PROXY_URL = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

async def test_google():
    print(f"[*] Testing Google Connectivity: {PROXY_URL}")
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
        page = await browser.new_page()
        
        try:
            print("[*] Navigating to https://www.google.com...")
            await page.goto("https://www.google.com", timeout=30000)
            title = await page.title()
            print(f"[*] Page Title: {title}")
            await page.screenshot(path="google_test.png")
        except Exception as e:
            print(f"[!] Google Test Error: {e}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_google())
