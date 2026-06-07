#!/usr/bin/env python3
"""
Write the authenticated Webshare proxy endpoint to a proxies file.

The legacy IP-authorization flow has been retired. We now use the standard
authenticated rotating endpoint on port 80, read from the PROXY_URL
environment variable:

    PROXY_URL=http://<username>-rotate:<password>@p.webshare.io:80
"""
import os

output_file = 'proxies_ip_auth.txt'

proxy_url = os.environ.get("PROXY_URL")
if not proxy_url:
    raise SystemExit("PROXY_URL is not set. Export it (see .env.example) before running.")

print("Writing authenticated proxy endpoint to file...")
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(proxy_url + "\n")

print(f"\u2713 Created {output_file}")
print(f"\u2713 Content: {proxy_url.split('@')[-1]} (credentials hidden)")
