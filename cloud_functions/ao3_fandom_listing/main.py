"""
ao3_fandom_listing — weekly census of AO3's canonical fandom index.

AO3 publishes every canonical fandom, with a work count, at
`/media/<Category>/fandoms`. Six pages cover ~59,000 fandoms. This function
snapshots them into dnd_trends_raw.ao3_fandom_totals.

WHY THIS EXISTS
    Three plan items in docs/data_capture_hardening_plan.md need this data, and
    a fourth is currently crippled without it:

      A  canonicality audit — a seed tag absent from the listing is a synonym
         or a non-common tag. 9 of 26 seed tags (35%) were wrong on Sep 2, 2026
         and every one produced a SILENT 0, indistinguishable from "this IP has
         no D&D crossover fic".
      C  denominators for the proportional crossover rate
      D  taxonomic level — which levels exist per franchise
      H  discovery census — a sampling frame that does not inherit the seed
         list's assumptions about what is licensable

    And the guard: gold_data.fanfic_capture_guard's METATAG_INFLATION check
    needs a fandom total to compare against. It currently resolves for 1 of 26
    AO3 IPs, because ao3_tag_counts covers only 23 D&D-native tags. That check
    is what would have caught the BG3 artifact (a "crossover count" of 49,020
    against a fandom of 48,997). Loading these totals turns it on for every IP.

ENDPOINT CLASS
    These are BROWSE pages — the same class ao3_harvester already scrapes on a
    schedule, and distinct from `/works?...` SEARCH queries, which stay
    human-wielded because they are expensive server-side. Same politeness
    contract as the sibling function: identifying User-Agent, 5s delay,
    429 backoff.

TIME SERIES
    A weekly snapshot is deliberate. Fandom sizes move slowly, but GROWTH is
    itself the signal work item H wants — a fast-rising fandom is a better
    sleeper candidate than a large static one, and you cannot compute growth
    from a single scrape.

NOTE ON DUPLICATION
    The parser is duplicated from scripts/ao3_fandom_listing.py. Cloud Functions
    deploy a self-contained directory and cannot import from ../scripts. The
    local script keeps an --offline mode for development; this is the production
    path. Changes to the parsing regex must be applied to both.
"""

import datetime
import html as html_mod
import json
import re
import time
import urllib.parse

import functions_framework
import requests
from google.cloud import bigquery

PROJECT = "dnd-trends-index"
TABLE = f"{PROJECT}.dnd_trends_raw.ao3_fandom_totals"

AO3_BASE = "https://archiveofourown.org"
REQUEST_DELAY = 5   # match ao3_harvester — be polite
INSERT_CHUNK = 500  # BigQuery streaming insert batch size

HEADERS = {
    "User-Agent": "DnD-Trends-Index/1.0 (research; contact: dnd-trends@example.com)",
    "Accept": "text/html",
}

# AO3 encodes '&' as '*a*' inside path segments.
CATEGORIES = [
    "Video Games",
    "Anime *a* Manga",
    "TV Shows",
    "Books *a* Literature",
    "Movies",
    "Cartoons *a* Comics *a* Graphic Novels",
]

UMBRELLA_SUFFIX = " - All Media Types"

# <a class="tag" href="/tags/Foo/works">Foo</a>&nbsp;(1,234)
ENTRY_RE = re.compile(
    r'<a[^>]+class="tag"[^>]*href="/tags/([^"]+?)(?:/works)?"[^>]*>(.*?)</a>'
    r'\s*(?:&nbsp;|\s)*\(\s*([\d,]+)\s*\)',
    re.IGNORECASE | re.DOTALL,
)
TAGSTRIP_RE = re.compile(r"<[^>]+>")


def _clean(s):
    # html.unescape handles NUMERIC entities. A hand-rolled replace() chain
    # mangles non-ASCII canonicals — 'Wied&#378;min | The Witcher' would not
    # match the seed tag 'Wiedźmin | The Witcher' and would be reported as
    # missing from the listing. That is the exact silent-mismatch class this
    # function exists to eliminate.
    return html_mod.unescape(TAGSTRIP_RE.sub("", s)).replace("\xa0", " ").strip()


def parse_listing(html, category):
    out, seen = [], set()
    for slug, name, count in ENTRY_RE.findall(html):
        name = _clean(name)
        if not name or name in seen:
            continue
        seen.add(name)
        out.append({
            "fandom": name,
            "category": category,
            "work_count": int(count.replace(",", "")),
            "ao3_slug": urllib.parse.unquote(slug),
            "is_umbrella": name.endswith(UMBRELLA_SUFFIX),
        })
    return out


def fetch_category(session, category):
    url = f"{AO3_BASE}/media/{urllib.parse.quote(category, safe='*')}/fandoms"
    resp = session.get(url, timeout=120)
    if resp.status_code == 429:
        print(f"  RATE LIMITED on {category}. Backing off 30s...")
        time.sleep(30)
        resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.text


class FandomListingHarvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.today = datetime.date.today()

    def run(self):
        print(f"--- AO3 Fandom Listing: {len(CATEGORIES)} categories ---")
        rows, failed = [], []

        for i, category in enumerate(CATEGORIES):
            try:
                parsed = parse_listing(fetch_category(self.session, category), category)
                if not parsed:
                    # An empty parse means the markup changed. Fail loudly —
                    # silently recording zero fandoms would be worse than an error.
                    failed.append(category)
                    print(f"  {category}: PARSED 0 ENTRIES — markup may have changed")
                else:
                    rows.extend(parsed)
                    print(f"  {category}: {len(parsed):,} fandoms")
            except Exception as e:
                failed.append(category)
                print(f"  {category}: FAILED — {e}")

            if i < len(CATEGORIES) - 1:
                time.sleep(REQUEST_DELAY)

        if not rows:
            raise RuntimeError("No fandoms parsed from any category — aborting insert.")

        stamp = self.today.isoformat()
        for r in rows:
            r["fetch_date"] = stamp

        inserted = 0
        for i in range(0, len(rows), INSERT_CHUNK):
            chunk = rows[i:i + INSERT_CHUNK]
            errors = self.bq.insert_rows_json(TABLE, chunk)
            if errors:
                print(f"  BQ insert errors at offset {i}: {errors[:2]}")
            else:
                inserted += len(chunk)

        top = sorted(rows, key=lambda r: -r["work_count"])[:5]
        print(f"\nInserted {inserted:,} of {len(rows):,} rows into ao3_fandom_totals.")
        print("--- Largest fandoms ---")
        for r in top:
            print(f"  {r['work_count']:>8,}  {r['fandom']}")

        return {
            "status": "success" if not failed else "partial",
            "categories_ok": len(CATEGORIES) - len(failed),
            "categories_failed": failed,
            "fandoms_parsed": len(rows),
            "rows_inserted": inserted,
            "umbrellas": sum(1 for r in rows if r["is_umbrella"]),
            "fetch_date": stamp,
        }


@functions_framework.http
def ao3_fandom_listing_http(request):
    """HTTP entry point for the AO3 fandom-listing census."""
    from shabbat_gate import is_shabbat, shabbat_skip_response
    if is_shabbat():
        return shabbat_skip_response()

    print("Starting AO3 Fandom Listing harvester...")
    try:
        result = FandomListingHarvester().run()
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
