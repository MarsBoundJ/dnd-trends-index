import asyncio
from playwright.async_api import async_playwright
import os

PROXY_URL = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

async def test_proxy():
    print(f"[*] Testing Proxy: {PROXY_URL}")
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
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            print("[*] Navigating to ifconfig.me...")
            await page.goto("http://ifconfig.me", timeout=30000)
            ip = await page.inner_text("body")
            print(f"[*] Current IP: {ip.strip()}")
            await page.screenshot(path="proxy_test.png")
            
        except Exception as e:
            print(f"[!] Proxy Test Error: {e}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_proxy())
