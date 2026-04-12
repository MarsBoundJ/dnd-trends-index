from pytrends.request import TrendReq
import json
p = 'http://oxsjenoi-residential-1-session-1762255993:yw72fdfu37vt@p.webshare.io:80'
pt = TrendReq(proxies={'https': p, 'http': p}, requests_args={'verify': True})
pt.build_payload(['dnd 5e'], timeframe='today 3-m', geo='US')
print('--- Related Topics ---')
try:
    rt = pt.related_topics()
    print(json.dumps(rt, indent=2, default=str))
except Exception as e:
    print(f'ERROR (Topics): {e}')

print('--- Related Queries ---')
try:
    rq = pt.related_queries()
    print(json.dumps(rq, indent=2, default=str))
except Exception as e:
    print(f'ERROR (Queries): {e}')
