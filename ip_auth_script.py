#!/usr/bin/env python3
"""
Create proxies file for IP authorization using port 9999
"""

# For IP authorization, we only need one entry since it's a rotating proxy pool
output_file = 'proxies_ip_auth.txt'

# IP authorization format: host:port (port 9999 for IP auth)
proxy_line = "p.webshare.io:9999\n"

print("Creating IP authorization proxy file...")
print(f"Format: host:port (using port 9999 for IP auth)")

# Since it's a rotating proxy, we only need one entry
# But we can add a few for redundancy
with open(output_file, 'w', encoding='utf-8') as f:
    # Just write one line - the proxy service handles rotation
    f.write(proxy_line)

print(f"✓ Created {output_file}")
print(f"✓ Content: {proxy_line.strip()}")
print("\nThis single entry will automatically rotate IPs on each request!")
print("Ready to use with the transcript fetching script.")
