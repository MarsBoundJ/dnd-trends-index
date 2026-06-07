import os
import asyncio
from playwright.async_api import async_playwright

async def test_proxy(proxy_cfg):
    print(f"Testing Proxy: {proxy_cfg['server']}")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                proxy=proxy_cfg
            )
            context = await browser.new_context(ignore_https_errors=True)
            page = await context.new_page()
            
            print("  [*] Navigating to trends.google.com...")
            response = await page.goto("https://trends.google.com/trends/explore?q=Fighter", timeout=30000)
            print(f"  [-] Status: {response.status}")
            print(f"  [-] Body: {await response.text()}")
            await browser.close()
    except Exception as e:
        print(f"  [!] Error: {e}")

async def main():
    from urllib.parse import urlparse
    _p = urlparse(os.environ.get("PROXY_URL", ""))
    proxy_options = [
        {"server": f"http://{_p.hostname}:{_p.port or 80}", "username": _p.username, "password": _p.password}
    ]
    for cfg in proxy_options:
        await test_proxy(cfg)

if __name__ == "__main__":
    asyncio.run(main())
