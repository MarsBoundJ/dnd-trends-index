�import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Test proxy from your proxies.txt file (using original username format)
proxies = {
    "http": "http://oxsjenoi-1:yw72fdfu37vt@p.webshare.io:80",
    "https": "http://oxsjenoi-1:yw72fdfu37vt@p.webshare.io:80",
}

print("=" * 60)
print("TESTING ORIGINAL USERNAME FORMAT: oxsjenoi-1")
print("=" * 60)

print("Testing proxy connection...")
print(f"Proxy: {proxies['http']}")

# Test 1: Simple HTTP request
try:
    print("\nTest 1: HTTP request to Google...")
    response = requests.head("http://clients3.google.com/generate_204", 
                            proxies=proxies, 
                            timeout=10,
                            verify=False)
    print(f"✓ HTTP test passed! Status code: {response.status_code}")
except Exception as e:
    print(f"✗ HTTP test failed: {e}")

# Test 2: HTTPS request
try:
    print("\nTest 2: HTTPS request to YouTube...")
    response = requests.head("https://www.youtube.com/generate_204", 
                            proxies=proxies, 
                            timeout=15,
                            verify=False)
    print(f"✓ HTTPS test passed! Status code: {response.status_code}")
except Exception as e:
    print(f"✗ HTTPS test failed: {e}")

# Test 3: Check IP
try:
    print("\nTest 3: Checking IP address...")
    response = requests.get("https://api.ipify.org?format=json", 
                           proxies=proxies, 
                           timeout=10,
                           verify=False)
    print(f"✓ Your proxy IP is: {response.json()['ip']}")
except Exception as e:
    print(f"✗ IP check failed: {e}")
�23file:///c:/Users/Yorri/PythonProjects/test_proxy.py