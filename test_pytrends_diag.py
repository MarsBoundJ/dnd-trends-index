from pytrends.request import TrendReq
import logging

# Enable logging to see the requests
logging.basicConfig(level=logging.DEBUG)

proxy_url = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

print("Starting pytrends diagnostic with residential proxy...")

try:
    # Try with single proxy dict
    pytrends = TrendReq(hl='en-US', tz=360, proxies=[proxy_url], timeout=(20, 60))
    
    print("Executing build_payload...")
    pytrends.build_payload(["Dungeons & Dragons"], cat=0, timeframe='today 1-m', geo='', gprop='')
    
    print("Fetching interest_over_time...")
    data = pytrends.interest_over_time()
    
    if data is not None and not data.empty:
        print(f"SUCCESS: Fetched {len(data)} rows.")
        print(data.tail())
    else:
        print("WARNING: Data is empty.")

except Exception as e:
    print(f"PYTRENDS FAILED: {e}")
    import traceback
    traceback.print_exc()
