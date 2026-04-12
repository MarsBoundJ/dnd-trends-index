import os

filepath = 'cloud_functions/related_queries_discovery/main.py'
with open(filepath, 'r') as f:
    content = f.read()

# Patch 1: fix _build_proxy_url to return None when no host
old1 = 'def _build_proxy_url() -> str:\n    # Webshare IP-authenticated endpoint — no user/pass needed, port 9999\n    return f"http://{PROXY_HOST}:{PROXY_PORT}"'
new1 = """def _build_proxy_url():
    if not PROXY_HOST or not PROXY_PORT:
        return None
    if PROXY_USER and PROXY_PASS:
        return f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return f"http://{PROXY_HOST}:{PROXY_PORT}\""""

# Patch 2: pass list of 50
old2 = '        proxies = [proxy_url]'
new2 = '        proxies = [proxy_url] * 50'

content = content.replace(old1, new1).replace(old2, new2)

with open(filepath, 'w') as f:
    f.write(content)

checks = ["if not PROXY_HOST" in content, "[proxy_url] * 50" in content]
print(f"Patch 1 (None check): {'OK' if checks[0] else 'FAILED'}")
print(f"Patch 2 (list*50): {'OK' if checks[1] else 'FAILED'}")
