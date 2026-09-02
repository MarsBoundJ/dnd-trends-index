"""
ao3_fandom_listing — scrape AO3's canonical fandom listings and answer work item D.

AO3 publishes a complete canonical-fandom index per media category at
`/media/<Category>/fandoms`, each entry carrying a work count. That single set of
pages supplies four things the capture pipeline needs:

  A  canonicality audit  — a tag absent from the listing is a synonym or non-common
  C  denominators        — fandom totals for the proportional crossover rate
  D  taxonomic level     — which levels exist per franchise (umbrella vs medium)
  H  discovery census    — every fandom, ranked, as a sampling frame

ENDPOINT CLASS: these are BROWSE pages, the same class as the tag pages that
`cloud_functions/ao3_harvester` already scrapes on a schedule — cheap, cacheable,
and distinct from `/works?...` SEARCH queries, which stay human-wielded because
they are expensive server-side. This script follows the harvester's conventions:
identifying User-Agent, 5s delay, 429 backoff.

Read-only. Writes a local JSON snapshot; does NOT write to BigQuery — review the
output first, then promote to a Cloud Function beside the existing harvester.

Usage:
    python scripts/ao3_fandom_listing.py                  # fetch + report
    python scripts/ao3_fandom_listing.py --out snap.json  # choose snapshot path
    python scripts/ao3_fandom_listing.py --offline DIR    # parse saved HTML instead
                                                          # (DIR/<Category>.html)
"""

from __future__ import annotations

import argparse
import datetime
import html as html_mod
import json
import pathlib
import re
import sys
import time
import urllib.parse

AO3_BASE = "https://archiveofourown.org"
REQUEST_DELAY = 5  # match cloud_functions/ao3_harvester — be polite

# TODO: replace with a real contact address before running at scale.
HEADERS = {
    "User-Agent": "DnD-Trends-Index/1.0 (research; contact: dnd-trends@example.com)",
    "Accept": "text/html",
}

# AO3 encodes '&' as '*a*' inside tag/category path segments.
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


# Entries look like:  <a class="tag" href="/tags/Foo/works">Foo</a>&nbsp;(1,234)
ENTRY_RE = re.compile(
    r'<a[^>]+class="tag"[^>]*href="/tags/([^"]+?)(?:/works)?"[^>]*>(.*?)</a>'
    r'\s*(?:&nbsp;|\s)*\(\s*([\d,]+)\s*\)',
    re.IGNORECASE | re.DOTALL,
)
TAGSTRIP_RE = re.compile(r"<[^>]+>")


def _clean(s: str) -> str:
    # html.unescape handles NUMERIC entities too. A hand-rolled replace() chain
    # silently mangles non-ASCII canonicals — 'Wied&#378;min | The Witcher' would
    # not match the seed tag 'Wiedźmin | The Witcher' and would be reported as
    # missing from the listing. Exactly the class of silent mismatch this whole
    # exercise exists to prevent.
    s = TAGSTRIP_RE.sub("", s)
    return html_mod.unescape(s).replace("\xa0", " ").strip()


def parse_listing(html: str, category: str) -> list[dict]:
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
            "slug": urllib.parse.unquote(slug),
        })
    return out


def fetch_category(session, category: str) -> str:
    path = urllib.parse.quote(category, safe="*")
    url = f"{AO3_BASE}/media/{path}/fandoms"
    resp = session.get(url, timeout=60)
    if resp.status_code == 429:
        print(f"  rate limited on {category} — backing off 30s", file=sys.stderr)
        time.sleep(30)
        resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


# ── Franchise grouping ──────────────────────────────────────────────────────
# A franchise is a SET of tags, not a tag. Where AO3 publishes an umbrella
# ("- All Media Types" or "& Related Fandoms") its wranglers have already done the entity
# resolution and the umbrella IS the union — use it. Children are detected by
# name prefix.
#
# CRITICAL: never sum children to reach a parent. Works carry multiple tags, so
# sum != union (observed: Anime 7,177 + Manga 8,051 = 15,228 vs umbrella 8,897).

def group_franchises(rows: list[dict]) -> dict[str, dict]:
    by_name = {r["fandom"]: r for r in rows}
    franchises: dict[str, dict] = {}

    for name, row in by_name.items():
        base = umbrella_base(name)
        if not base:
            continue
        children = [
            r for n, r in by_name.items()
            if n != name and (n == base or n.startswith(base + " ") or n.startswith(base + "("))
        ]
        franchises[base] = {
            "umbrella": row,
            "children": sorted(children, key=lambda r: -r["work_count"]),
            "level_available": "umbrella",
        }

    claimed = {c["fandom"] for f in franchises.values() for c in f["children"]}
    claimed |= {f["umbrella"]["fandom"] for f in franchises.values()}
    for name, row in by_name.items():
        if name not in claimed:
            franchises.setdefault(name, {
                "umbrella": None, "children": [row], "level_available": "single_canonical",
            })
    return franchises


def classify_seed_tags(franchises: dict[str, dict]) -> list[dict]:
    """For each seed tag, report the level we currently measure and what else exists."""
    try:
        sys.path.insert(0, str(pathlib.Path(__file__).parent))
        from seed_fanfic_canonical_tags import MAPPINGS
    except Exception as e:  # pragma: no cover
        print(f"(could not import seed list: {e})", file=sys.stderr)
        return []

    index = {}
    for base, f in franchises.items():
        if f["umbrella"]:
            index[f["umbrella"]["fandom"]] = (base, "umbrella", f)
        for c in f["children"]:
            index.setdefault(c["fandom"], (base, "child", f))

    report = []
    for m in MAPPINGS:
        hit = index.get(m.ao3_tag)
        if not hit:
            report.append({
                "ip_name": m.ip_name, "our_tag": m.ao3_tag, "status": "NOT FOUND IN LISTING",
                "our_level": None, "our_count": None, "umbrella": None, "siblings": [],
            })
            continue
        base, level, f = hit
        our = f["umbrella"] if level == "umbrella" else next(
            c for c in f["children"] if c["fandom"] == m.ao3_tag)
        report.append({
            "ip_name": m.ip_name,
            "our_tag": m.ao3_tag,
            "status": "ok",
            "our_level": level,
            "our_count": our["work_count"],
            "umbrella": (f["umbrella"]["fandom"] if f["umbrella"] else None),
            "umbrella_count": (f["umbrella"]["work_count"] if f["umbrella"] else None),
            "siblings": [c["fandom"] for c in f["children"] if c["fandom"] != m.ao3_tag],
        })
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="scratch/ao3_fandom_listing.json")
    ap.add_argument("--offline", metavar="DIR",
                    help="parse saved HTML from DIR/<Category>.html instead of fetching")
    args = ap.parse_args()

    rows: list[dict] = []
    if args.offline:
        d = pathlib.Path(args.offline)
        for cat in CATEGORIES:
            f = d / f"{cat.replace('*a*', 'and').replace(' ', '_')}.html"
            if not f.exists():
                print(f"skip (missing) {f}", file=sys.stderr)
                continue
            rows += parse_listing(f.read_text(encoding="utf-8", errors="replace"), cat)
            print(f"parsed {cat}: {len(rows)} cumulative")
    else:
        import requests  # imported lazily so --offline needs no dependency
        session = requests.Session()
        session.headers.update(HEADERS)
        for i, cat in enumerate(CATEGORIES):
            print(f"fetching {cat} …")
            rows += parse_listing(fetch_category(session, cat), cat)
            print(f"  {len(rows)} cumulative")
            if i < len(CATEGORIES) - 1:
                time.sleep(REQUEST_DELAY)

    if not rows:
        print("No rows parsed — the listing markup may have changed.", file=sys.stderr)
        sys.exit(1)

    franchises = group_franchises(rows)
    seed_report = classify_seed_tags(franchises)

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
        "categories": CATEGORIES,
        "fandom_count": len(rows),
        "fandoms": sorted(rows, key=lambda r: -r["work_count"]),
        "seed_tag_report": seed_report,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── Item D report ──
    print(f"\n{len(rows)} canonical fandoms across {len(CATEGORIES)} categories")
    print(f"snapshot: {out}\n")
    print("=== ITEM D — level we currently measure, per seed IP ===")
    mism = 0
    for r in sorted(seed_report, key=lambda r: (r["status"] != "ok", r["ip_name"])):
        if r["status"] != "ok":
            print(f"  !! {r['ip_name']:<34} {r['our_tag']}  <-- NOT IN LISTING")
            mism += 1
            continue
        flag = ""
        if r["our_level"] == "child" and r["umbrella"]:
            flag = f"  <-- umbrella exists: {r['umbrella']} ({r['umbrella_count']:,})"
            mism += 1
        print(f"  {r['ip_name']:<34} {r['our_level']:<6} {r['our_count']:>8,}{flag}")
    print(f"\n{mism} seed tags are NOT at umbrella level (or are missing).")
    print("Levels overlap — never sum children to reach a parent.")


if __name__ == "__main__":
    main()
