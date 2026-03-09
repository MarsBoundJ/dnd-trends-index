import os
import time
from pytrends.request import TrendReq

# IP AUTH PROXY
PROXY_URL = "http://p.webshare.io:9999"

def main():
    print("Testing NPC Title + 'class' qualifiers...")
    keywords = [
        "Captain class 5e", "Captain class dnd",
        "Merchant class 5e", "Merchant class dnd",
        "Scholar class 5e", "Scholar class dnd"
    ]
    proxies = [PROXY_URL]
    
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=5, backoff_factor=2, proxies=proxies)
        print("TrendReq object created with proxy.")
        
        # Batching 5 at a time (standard pytrends limit for one request)
        print("Fetching first batch...")
        pytrends.build_payload(keywords[:5], cat=0, timeframe='today 12-m', geo='', gprop='')
        data1 = pytrends.interest_over_time()
        
        print("Fetching second batch...")
        pytrends.build_payload([keywords[5]], cat=0, timeframe='today 12-m', geo='', gprop='')
        data2 = pytrends.interest_over_time()
        
        results = {}
        if not data1.empty:
            for kw in keywords[:5]:
                if kw in data1.columns:
                    results[kw] = data1[kw].mean()
        
        if not data2.empty:
            kw = keywords[5]
            if kw in data2.columns:
                results[kw] = data2[kw].mean()

        print("\nExperimental Results (Mean Interest):")
        for kw, score in results.items():
            print(f"  {kw}: {score:.4f}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
