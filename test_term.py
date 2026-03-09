from pytrends.request import TrendReq
import logging

proxy_url = "http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80"

term = "D&D Battle Maps"
start_date = '2026-02-01'
end_date = '2026-03-01'

print(f"Testing term: {term}")

try:
    pytrends = TrendReq(hl='en-US', tz=360, proxies=[proxy_url], timeout=(20, 60))
    pytrends.build_payload([term], cat=0, timeframe=f'{start_date} {end_date}', geo='', gprop='')
    data = pytrends.interest_over_time()
    
    if data is not None:
        print(f"Data shape: {data.shape}")
        if not data.empty:
            print(data.head())
        else:
            print("Data is EMPTY.")
    else:
        print("Data is NONE.")

except Exception as e:
    print(f"FAILED: {e}")
