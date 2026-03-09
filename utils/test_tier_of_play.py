import os
import time
from pytrends.request import TrendReq

# IP AUTH PROXY (matches recover_global_pillars.py)
PROXY_URL = "http://p.webshare.io:9999"

def main():
    print("Testing 'Tier of Play' variant with Proxy...")
    proxies = [PROXY_URL]
    keywords = ["Tier of Play 5e", "Tier of Play dnd", "Tiers of Play 5e", "Tiers of Play dnd"]
    
    try:
        # Use simple proxy list passed to TrendReq
        pytrends = TrendReq(hl='en-US', tz=360, proxies=proxies)
        print("TrendReq object created with proxy.")
        pytrends.build_payload(keywords, cat=0, timeframe='today 12-m', geo='', gprop='')
        print("Payload built.")
        data = pytrends.interest_over_time()
        print("Data fetched.")
        
        if not data.empty:
            print("\nScores (Annual Mean / 100):")
            for kw in keywords:
                if kw in data.columns:
                    print(f"  {kw}: {data[kw].mean():.2f}")
        else:
            print("No data returned (possibly zero interest).")
            
    except Exception as e:
        print(f"Caught exception: {e}")

if __name__ == "__main__":
    main()
