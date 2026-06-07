import datetime
from pytrends.request import TrendReq
import time
import os

PROXY_URL = os.environ.get("PROXY_URL")

def test_pytrends_auth():
    print(f"[*] Testing Pytrends with Auth Proxy: {PROXY_URL}")
    term = "Daggerheart"
    
    try:
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(25, 60), proxies=[PROXY_URL])
        pytrends.build_payload([term], timeframe='2026-02-01 2026-03-01', geo='US')
        data = pytrends.interest_over_time()
        
        if data is None or data.empty:
            print(f"  [-] No data found for {term}")
        else:
            print(f"  [+] SUCCESS! Captured {len(data)} rows.")
            print(data.head())
            
    except Exception as e:
        print(f"  [!] Error: {e}")

if __name__ == "__main__":
    test_pytrends_auth()
