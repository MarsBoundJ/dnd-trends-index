
import requests
import os

# Test 1: Original Rotating Endpoint (with '-rota' suffix)
print("=== Test 1: Rotating Endpoint (Original) ===")
proxy_url_rota = os.environ.get("PROXY_URL")
print(f"Proxy: {proxy_url_rota.split('@')[1]}")
proxies = {"http": proxy_url_rota, "https": proxy_url_rota}
try:
    response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=15)
    response.raise_for_status()
    print(f"SUCCESS! IP: {response.json()['ip']}")
except Exception as e:
    print(f"FAILED: {e}")

# Test 2: Rotating Endpoint WITHOUT the '-rota' suffix (base username)
print("\n=== Test 2: Base Username (no suffix) ===")
proxy_url_base = os.environ.get("PROXY_URL")
print(f"Proxy: {proxy_url_base.split('@')[1]}")
proxies = {"http": proxy_url_base, "https": proxy_url_base}
try:
    response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=15)
    response.raise_for_status()
    print(f"SUCCESS! IP: {response.json()['ip']}")
except Exception as e:
    print(f"FAILED: {e}")

# Test 3: Static proxy (known working - control)
print("\n=== Test 3: Static Proxy -1 (Control) ===")
proxy_url_static = os.environ.get("PROXY_URL")
print(f"Proxy: {proxy_url_static.split('@')[1]}")
proxies = {"http": proxy_url_static, "https": proxy_url_static}
try:
    response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=15)
    response.raise_for_status()
    print(f"SUCCESS! IP: {response.json()['ip']}")
except Exception as e:
    print(f"FAILED: {e}")
