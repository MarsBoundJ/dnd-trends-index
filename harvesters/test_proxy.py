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
    proxy_options = [
        {"server": "socks5://p.webshare.io:1080", "username": "lcbaurkt-US-rotate", "password": "q8aa993piq8h"},
        {"server": "http://p.webshare.io:80", "username": "lcbaurkt-US-rotate", "password": "q8aa993piq8h"}
    ]
    for cfg in proxy_options:
        await test_proxy(cfg)

if __name__ == "__main__":
    asyncio.run(main())
