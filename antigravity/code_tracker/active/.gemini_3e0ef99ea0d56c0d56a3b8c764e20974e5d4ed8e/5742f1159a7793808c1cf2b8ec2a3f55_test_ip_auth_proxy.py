¶
import requests
import time

# IP Auth Proxy Endpoint
# No username/password needed if IP is whitelisted
PROXY_URL = "http://p.webshare.io:9999"

print(f"Testing IP Auth Proxy: {PROXY_URL}")

proxies = {
    "http": PROXY_URL,
    "https": PROXY_URL,
}

def test_connection():
    try:
        print("\nAttempting to reach api.ipify.org...")
        start_time = time.time()
        response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=15)
        response.raise_for_status()
        duration = time.time() - start_time
        
        print(f"‚úÖ Success! ({duration:.2f}s)")
        print(f"External IP seen by target: {response.json()['ip']}")
        return True
    except Exception as e:
        print(f"‚ùå Connection Failed: {e}")
        return False

if __name__ == "__main__":
    # Try a few times to see if rotation works (IPs should change)
    for i in range(3):
        print(f"\n--- Request {i+1} ---")
        if test_connection():
             pass
        time.sleep(1)
¶*cascade08"(3e0ef99ea0d56c0d56a3b8c764e20974e5d4ed8e24file:///C:/Users/Yorri/.gemini/test_ip_auth_proxy.py:file:///C:/Users/Yorri/.gemini