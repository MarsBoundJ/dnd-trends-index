‡
import requests
import os

# Test 1: Original Rotating Endpoint (with '-rota' suffix)
print("=== Test 1: Rotating Endpoint (Original) ===")
proxy_url_rota = "http://oxsjenoi-residential-rota:yw72fdfu37vt@p.webshare.io:80"
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
proxy_url_base = "http://oxsjenoi-residential:yw72fdfu37vt@p.webshare.io:80"
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
proxy_url_static = "http://oxsjenoi-residential-1:yw72fdfu37vt@p.webshare.io:80"
print(f"Proxy: {proxy_url_static.split('@')[1]}")
proxies = {"http": proxy_url_static, "https": proxy_url_static}
try:
    response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=15)
    response.raise_for_status()
    print(f"SUCCESS! IP: {response.json()['ip']}")
except Exception as e:
    print(f"FAILED: {e}")
‡*cascade08"(ebeb31169344ecade5342e92de1cbad75038dafa2-file:///C:/Users/Yorri/.gemini/debug_proxy.py:file:///C:/Users/Yorri/.gemini