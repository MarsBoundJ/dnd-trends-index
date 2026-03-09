from pytrends.request import TrendReq
import time

kw = 'pizza'
print(f"Testing {kw}...")
try:
    pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 10))
    pytrends.build_payload([kw], cat=0, timeframe='today 12-m')
    df = pytrends.interest_over_time()
    if not df.empty:
        print(f"Success! Found {len(df)} rows.")
    else:
        print("Empty results.")
except Exception as e:
    print(f"Error: {e}")
