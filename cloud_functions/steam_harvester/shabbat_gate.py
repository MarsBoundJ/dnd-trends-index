"""
shabbat_gate.py — Shabbat-observant scraping gate for Arcane Analytics.

Blackout window (fixed, covers all year for Council Bluffs & Cedar Rapids, Iowa):
  - START: Friday 21:30 UTC  (= earliest sundown in Cedar Rapids ~4:30 PM CST, Dec)
  - END:   Sunday 03:45 UTC  (= latest halachic twilight in Council Bluffs ~9:45 PM CDT, Jun)

When SHABBAT_MODE=true (default), functions called during the blackout
return early without scraping. Set SHABBAT_MODE=false to disable the gate
(e.g., if the project is sold to a non-observant owner).

Usage in a Cloud Function:
    from shabbat_gate import is_shabbat, shabbat_skip_response

    @functions_framework.http
    def my_harvester(request):
        if is_shabbat():
            return shabbat_skip_response()
        # ... normal harvesting logic ...
"""

import os
import json
import datetime

# Toggle: set to "false" to disable Shabbat observance
SHABBAT_MODE = os.environ.get("SHABBAT_MODE", "true").lower() == "true"

# Blackout window in UTC (fixed worst-case for Iowa across the full year)
# Friday 21:30 UTC = earliest Friday sundown in Iowa (summer CDT ~4:30 PM)
# Sunday 03:45 UTC = latest Saturday night halachic twilight in Iowa (winter CST ~9:45 PM)
BLACKOUT_START_DOW = 4   # Friday (Monday=0 in Python's weekday())
BLACKOUT_START_HOUR = 21
BLACKOUT_START_MINUTE = 30

BLACKOUT_END_DOW = 6     # Sunday
BLACKOUT_END_HOUR = 3
BLACKOUT_END_MINUTE = 45


def is_shabbat(now=None):
    """
    Check if the current time falls within the Shabbat blackout window.
    Returns False immediately if SHABBAT_MODE is disabled.
    """
    if not SHABBAT_MODE:
        return False

    if now is None:
        now = datetime.datetime.utcnow()

    dow = now.weekday()  # Monday=0, Sunday=6
    t = now.hour * 60 + now.minute  # minutes since midnight

    blackout_start = BLACKOUT_START_HOUR * 60 + BLACKOUT_START_MINUTE  # 21:30 = 1290
    blackout_end = BLACKOUT_END_HOUR * 60 + BLACKOUT_END_MINUTE        # 03:45 = 225

    # Friday after 21:30 UTC
    if dow == BLACKOUT_START_DOW and t >= blackout_start:
        return True

    # All of Saturday
    if dow == 5:  # Saturday
        return True

    # Sunday before 03:45 UTC
    if dow == BLACKOUT_END_DOW and t < blackout_end:
        return True

    return False


def shabbat_skip_response():
    """Return a JSON response indicating the function skipped due to Shabbat."""
    msg = {
        "status": "skipped",
        "reason": "Shabbat observance",
        "message": "Scraping paused for Shabbat. Will resume Saturday night after halachic twilight.",
        "shabbat_mode": True,
    }
    print(f"Shabbat gate: skipping run. {json.dumps(msg)}")
    return json.dumps(msg), 200, {"Content-Type": "application/json"}
