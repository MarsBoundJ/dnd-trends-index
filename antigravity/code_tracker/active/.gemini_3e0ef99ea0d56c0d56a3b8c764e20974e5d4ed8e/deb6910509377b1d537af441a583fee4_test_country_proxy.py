�
import requests
import time

# Base Config
PASSWORD = "yw72fdfu37vt"
HOST = "p.webshare.io:80"

# Variants to test
usernames = [
    "oxsjenoi-US",
    "oxsjenoi-residential-US",
    "oxsjenoi-residential-rota-US",
    "oxsjenoi-rotate-US"
]

def check_proxy(username):
    proxy_url = f"http://{username}:{PASSWORD}@{HOST}"
    proxies = {"http": proxy_url, "https": proxy_url}
    
    print(f"\nTesting Username: {username}")
    try:
        start = time.time()
        # Check IP/Location
        resp = requests.get("http://ip-api.com/json", proxies=proxies, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        duration = time.time() - start
        country = data.get('countryCode')
        
        print(f"✅ SUCCESS! ({duration:.2f}s)")
        print(f"   IP: {data.get('query')}")
        print(f"   Location: {data.get('country')} ({country})")
        
        if country == 'US':
            print("   🎯 TARGET MATCHED! (US Proxy Confirmed)")
            return True
        else:
            print("   ⚠️  Matched, but wrong country.")
            return False
            
    except Exception as e:
        print(f"❌ FAILED: {str(e)[:100]}")
        return False

print("=== Testing Country-Specific Proxy Auth ===")
print("Goal: Find a format that forces US location.")

success = False
for u in usernames:
    if check_proxy(u):
        success = True
        break

if not success:
    print("\nNo username format worked for US filtering. We might need to rely on Dashboard settings (Port 9999).")
�*cascade08"(3e0ef99ea0d56c0d56a3b8c764e20974e5d4ed8e24file:///C:/Users/Yorri/.gemini/test_country_proxy.py:file:///C:/Users/Yorri/.gemini