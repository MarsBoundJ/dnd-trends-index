from pytrends.request import TrendReq
import time

try:
    print("Testing Pytrends...")
    pytrends = TrendReq(hl='en-US', tz=360)
    print("Payload building...")
    pytrends.build_payload(['dnd 5e'], cat=0, timeframe='today 12-m', geo='', gprop='')
    print("Interest over time...")
    data = pytrends.interest_over_time()
    print("Result:")
    print(data.head())
except Exception as e:
    print(f"Error: {e}")
