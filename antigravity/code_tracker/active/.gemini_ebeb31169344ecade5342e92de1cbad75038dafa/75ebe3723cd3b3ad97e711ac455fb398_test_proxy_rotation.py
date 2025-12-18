«

import time
import random
from pytrends.request import TrendReq

# Pool of static proxies to rotate through
PROXY_POOL = [
    "http://oxsjenoi-residential-1:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-2:yw72fdfu37vt@p.webshare.io:80",
    "http://oxsjenoi-residential-3:yw72fdfu37vt@p.webshare.io:80",
]

test_terms = [
    ["Wizard 5e", "Wizard 2024", "Wizard build", "Fighter 5e", "Fighter 2024"],
    ["Rogue 5e", "Rogue build", "Paladin 5e", "Paladin build", "Monk 5e"],
]

print("=== Proxy Rotation Test for Pytrends ===")

for idx, batch in enumerate(test_terms):
    proxy = PROXY_POOL[idx % len(PROXY_POOL)]
    print(f"\nBatch {idx+1}: {batch[:2]}... using proxy: {proxy.split('@')[1]}")
    
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=2, backoff_factor=0.5, proxies=[proxy])
        pytrends.build_payload(batch, cat=0, timeframe='today 12-m', geo='', gprop='')
        data = pytrends.interest_over_time()
        
        if data.empty:
            print("  No data returned.")
        else:
            print(f"  SUCCESS! Got {len(data)} rows, columns: {list(data.columns)[:3]}")
    except Exception as e:
        print(f"  FAILED: {str(e)[:100]}")
    
    time.sleep(random.uniform(5, 10))

print("\n=== Test Complete ===")
«
*cascade08"(ebeb31169344ecade5342e92de1cbad75038dafa25file:///C:/Users/Yorri/.gemini/test_proxy_rotation.py:file:///C:/Users/Yorri/.gemini