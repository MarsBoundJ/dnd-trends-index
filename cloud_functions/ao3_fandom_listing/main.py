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

# Explicit schema: a load job against a partition decorator cannot autodetect.
TABLE_SCHEMA = [
    bigquery.SchemaField("fandom", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("work_count", "INT64"),
    bigquery.SchemaField("ao3_slug", "STRING"),
    bigquery.SchemaField("is_umbrella", "BOOL"),
    bigquery.SchemaField("umbrella_kind", "STRING"),
    bigquery.SchemaField("fetch_date", "DATE", mode="REQUIRED"),
]

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

# AO3 publishes umbrellas under TWO suffixes, not one.
#
#   "<Name> - All Media Types"   same entity, aggregated across media (257 tags)
#   "<Name> & Related Fandoms"   entity plus its spin-offs/related works (65 tags)
#
# Only the first was recognised until Sep 2, 2026, so 65 umbrellas — 20% of all
# 322 — were flagged is_umbrella=False. That is not cosmetic: it hid the fact
# that Doctor Who was being measured at "Doctor Who (2005)" (61,401 works) while
# "Doctor Who & Related Fandoms" (109,819) sat unused, and it mislabelled Avatar
# as non-umbrella when it was already correctly at the broadest level.
#
# The two forms are NOT interchangeable and downstream consumers may care which
# one they got, so the distinction is preserved in umbrella_kind rather than
# collapsed into the boolean.
UMBRELLA_SUFFIXES = (" - All Media Types", "& Related Fandoms")


def umbrella_kind(name: str) -> str:
    """'all_media' | 'related_fandoms' | '' — '' means not an umbrella."""
    if name.endswith(" - All Media Types"):
        return "all_media"
    if name.endswith("& Related Fandoms"):
        return "related_fandoms"
    return ""


def umbrella_base(name: str) -> str:
    """The franchise stem, or '' if name is not an umbrella."""
    for suf in UMBRELLA_SUFFIXES:
        if name.endswith(suf):
            return name[: -len(suf)].strip()
    return ""


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
            "is_umbrella": bool(umbrella_kind(name)),
            "umbrella_kind": umbrella_kind(name) or None,
        })
    return out


def fetch_category(session, category):
    # 120s was not enough — the Movies listing timed out at exactly that on
    # Sep 2, 2026. These pages are genuinely large (Video Games alone carries
    # ~8,300 entries), so allow 180s and retry once on any transport error,
    # not just on 429.
    url = f"{AO3_BASE}/media/{urllib.parse.quote(category, safe='*')}/fandoms"

    def _get():
        r = session.get(url, timeout=180)
        if r.status_code == 429:
            print(f"  RATE LIMITED on {category}. Backing off 30s...")
            time.sleep(30)
            r = session.get(url, timeout=180)
        r.raise_for_status()
        return r

    try:
        return _get().text
    except Exception as e:
        # Single retry after a pause. Deliberately not a retry loop: repeated
        # hammering of a slow endpoint is how a soft throttle becomes a hard
        # block (see feedback_ddb_homebrew_cadence).
        print(f"  {category}: {type(e).__name__} — one retry in 15s")
        time.sleep(15)
        return _get().text


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

        # REFUSE TO WRITE A PARTIAL SNAPSHOT.
        #
        # Learned the hard way on Sep 2, 2026: the partition-replacing write
        # below is correct for a COMPLETE run and destructive for a partial one.
        # A transient AO3 read timeout on one category produced 45,123 rows, and
        # WRITE_TRUNCATE cheerfully replaced a complete 59,143-row snapshot with
        # it. The idempotency fix had turned a recoverable hiccup into data loss.
        #
        # A partial census is not merely incomplete, it is WRONG in a direction
        # that matters: missing fandoms become missing denominators, which
        # surface as NO_FANDOM_TOTAL or, worse, silently shrink the population
        # rate. Better to keep last week's complete snapshot and fail loudly.
        if failed:
            raise RuntimeError(
                f"Refusing to write a partial snapshot: {len(failed)} of "
                f"{len(CATEGORIES)} categories failed ({', '.join(failed)}). "
                f"Parsed {len(rows):,} rows but did not write — the existing "
                f"partition is left intact. Retry on the next scheduled run."
            )

        stamp = self.today.isoformat()
        for r in rows:
            r["fetch_date"] = stamp

        # IDEMPOTENT WRITE — replace today's partition rather than appending.
        #
        # Cloud Scheduler retries on failure, and the previous streaming-append
        # implementation double-counted the day on any re-run: a partial failure
        # followed by a retry, or a manual invoke, left two snapshots in one
        # partition. The views dedupe, so this never corrupted a result — but a
        # write that is wrong-but-survivable is still wrong, and the raw table
        # grows without bound on a retry loop.
        #
        # A load job against the partition decorator (table$YYYYMMDD) with
        # WRITE_TRUNCATE makes re-running a no-op rather than a duplication.
        # Load jobs are also free and skip the streaming buffer, which would
        # otherwise block DML against the table for ~90 minutes after each run.
        partition = f"{TABLE}${stamp.replace('-', '')}"
        job = self.bq.load_table_from_json(
            rows,
            partition,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=TABLE_SCHEMA,
            ),
        )
        job.result()
        inserted = len(rows)

        top = sorted(rows, key=lambda r: -r["work_count"])[:5]
        print(f"\nWrote {inserted:,} rows to ao3_fandom_totals partition {stamp} (replaced).")
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
