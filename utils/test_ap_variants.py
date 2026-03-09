import os
import time
from pytrends.request import TrendReq

# IP AUTH PROXY
PROXY_URL = "http://p.webshare.io:9999"

def main():
    print("Testing Variants Tool")
    proxies_list = [PROXY_URL] * 5
    pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=1, proxies=proxies_list)
    
    # Testing variants for Avantris Surpasa
    test_terms = [
        "Legends of Avantris", # Baseline
        "Legends of Avantris Surpasa",
        "Avantris Surpasa",
        "Titansgrave",
        "Titansgrave: The Ashes of Valkana"
    ]
    
    print(f"Testing: {test_terms}")
    pytrends.build_payload(test_terms, cat=0, timeframe='today 12-m', geo='', gprop='')
    data = pytrends.interest_over_time()
    
    if not data.empty:
        for term in test_terms:
            avg = data[term].mean()
            print(f"{term}: {avg:.2f}")
    else:
        print("No data returned.")

if __name__ == "__main__":
    main()
