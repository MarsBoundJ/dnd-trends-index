"""Guards the BGG/RPGGeek harvester's request host.

    python scripts/test_bgg_harvester_host.py

WHY THIS EXISTS. From ~Sep 4 to Sep 17 2026 `rpggeek_product_stats` wrote nothing
while `bgg_product_stats` kept filling normally from the SAME function, proxy and
token. The RPG branch fetched from `rpggeek.com`; the BGG branch from
`boardgamegeek.com`.

Our Bearer token is only honoured on boardgamegeek.com -- BGG's own API page says
"please ensure that you are making your requests to the correct domain
(boardgamegeek.com, WITHOUT a leading www)". rpggeek.com sits behind a Cloudflare
bot challenge that returns 403 with a "Just a moment..." interstitial before the
API layer ever sees the Authorization header.

The failure was invisible from outside: the Cloud Run request returned HTTP 200
in 66s and the scheduler recorded no error, because every one of the 21 per-ID
403s was swallowed into `return None`. Only the empty table gave it away.

These checks are static -- no network -- so they run anywhere and cannot flake.
"""

import io
import os
import re
import sys

SRC = os.path.join(os.path.dirname(__file__), "..", "cloud_functions", "bgg_harvester", "main.py")
SOURCE = io.open(SRC, encoding="utf-8").read()

# Strip comments so the explanatory block above the fix cannot satisfy or break a
# check. A guard that passes because of a comment is not a guard.
CODE = re.sub(r"(?m)#.*$", "", SOURCE)

passed = failed = 0


def check(name, actual, expected):
    global passed, failed
    if actual == expected:
        passed += 1
        print("  ok   " + name)
    else:
        failed += 1
        print(f"  FAIL {name}\n         expected {expected!r}, got {actual!r}")


print("\nRequest host:")
check("no code path fetches from rpggeek.com",
      "rpggeek.com" in CODE, False)
check("boardgamegeek.com is the base URL",
      'base_url = "https://boardgamegeek.com/xmlapi2/thing"' in CODE, True)
check("exactly one base_url assignment (one host for both platforms)",
      len(re.findall(r"base_url\s*=", CODE)), 1)
check("no leading www. on the API host (the token is rejected with it)",
      "www.boardgamegeek.com" in CODE, False)

print("\nRPG items are still selected, just not by hostname:")
check("type=rpgitem is applied when is_rpg", "&type=rpgitem" in CODE, True)
check("the rpg branch still targets the rpggeek table",
      "dnd_trends_raw.rpggeek_product_stats" in CODE, True)
check("the bgg branch still targets the bgg table",
      "dnd_trends_raw.bgg_product_stats" in CODE, True)

print("\nAuthorization:")
check("an Authorization Bearer header is sent",
      'Authorization": f"Bearer' in CODE, True)

print(f"\n{passed} passed, {failed} failed\n")
sys.exit(1 if failed else 0)
