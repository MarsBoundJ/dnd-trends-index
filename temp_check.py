#!/usr/bin/env python3
"""
Help determine the correct proxy username format

Based on Webshare.io documentation, the username format depends on the proxy type:

1. DATACENTER PROXIES:
   Format: username-country-session
   Example: myuser-us-session-12345

2. RESIDENTIAL PROXIES (Rotating):
   Format: username-residential-rotate
   Example: myuser-residential-rotate
   
3. RESIDENTIAL PROXIES (Sticky Session):
   Format: username-residential-session-TIME
   Example: myuser-residential-session-15m (15 minute session)
   
4. STATIC RESIDENTIAL PROXIES:
   Format: username-residential-COUNTRY-CITY-SESSION
   Example: myuser-residential-us-newyork-session-1

Your current username base: oxsjenoi
Your password: yw72fdfu37vt
Your proxy host: p.webshare.io:80

QUESTIONS TO DETERMINE CORRECT FORMAT:
======================================

1. What type of proxies did you purchase from Webshare?
   - Datacenter proxies
   - Residential rotating proxies
   - Residential static proxies
   - ISP proxies

2. Do you have access to your Webshare dashboard?
   - Check: https://proxy.webshare.io/
   - Look for "Proxy List" or "Download List"
   - The correct format will be shown there

3. Check your Webshare email confirmation
   - When you purchased the proxies, Webshare sent setup instructions
   - The email contains the exact username format to use

COMMON ISSUES:
==============
- Username format must match your proxy type
- The numbered format (oxsjenoi-1, oxsjenoi-2) suggests these might be 
  individual static proxies, not a rotating pool
- If you have 215,000 lines, these are likely individual proxy IPs,
  each with its own unique identifier

NEXT STEPS:
===========
1. Log into https://proxy.webshare.io/
2. Go to "Proxy List" or "Proxy Configuration"
3. Download or copy the proxy list in the correct format
4. Replace your proxies.txt with the correct format

Alternatively, check your purchase confirmation email for the exact format.
"""

print(__doc__)

# Try to detect from the file
print("\n" + "="*70)
print("ANALYZING YOUR CURRENT PROXIES.TXT FILE")
print("="*70)

try:
    with open('proxies.txt', 'r') as f:
        first_line = f.readline().strip()
        total_lines = sum(1 for _ in f) + 1
    
    print(f"\nFirst proxy line: {first_line}")
    print(f"Total proxy lines: {total_lines:,}")
    
    parts = first_line.split(':')
    if len(parts) == 4:
        host, port, username, password = parts
        print(f"\nParsed format:")
        print(f"  Host: {host}")
        print(f"  Port: {port}")
        print(f"  Username: {username}")
        print(f"  Password: {password[:4]}****")
        
        print(f"\n⚠ ANALYSIS:")
        print(f"  You have {total_lines:,} proxy lines.")
        print(f"  This suggests you have INDIVIDUAL STATIC PROXIES,")
        print(f"  not a rotating pool.")
        print(f"\n  The username format '{username}' needs to be verified")
        print(f"  against your Webshare account settings.")
        
except FileNotFoundError:
    print("proxies.txt not found")
except Exception as e:
    print(f"Error reading file: {e}")
