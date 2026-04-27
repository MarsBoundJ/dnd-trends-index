"""
Arcane Analytics — find BGG IDs for licensed board game adaptations of
UB candidate IPs (Stage 3 prep, Apr 27, 2026).

For each IP in scripts/seed_ub_candidate_ips.py, queries BGG's public
search API for "[IP] board game" matches, fetches owner counts to rank
candidates, and emits a JSON proposal of top-3 matches per IP.

The output is REVIEW MATERIAL, not a final mapping. Phil + Claude
hand-curate scripts/seed_ub_bgg_proxy_ids.py from this output —
choosing the right BGG ID per IP, dropping IPs without a real licensed
board game adaptation.

Usage:
    python scripts/find_ub_bgg_ids.py                  # write to default file
    python scripts/find_ub_bgg_ids.py --limit 10       # quick test
    python scripts/find_ub_bgg_ids.py --output X.json  # custom path

Why this is review material, not direct curation:
- BGG search returns lots of false positives (community-made expansions,
  print-and-play knockoffs, video game ports under similar names).
- Some IPs have multiple legitimate licensed board games (LotR has many)
  and we want to pick the most representative one or include several.
- Some IPs have NO licensed board game (most webtoons, most literary
  fiction), and the search will return junk we should drop entirely.

Cost: free. 2 API calls per IP (search + batched stats), each with
2s rate limit → ~4s wall-clock per IP. Full 142-IP run = ~10 min.

Recommended: pass --names "ip1,ip2,ip3" to query just specific IPs
when filling gaps in hand-curated mappings (run finishes in seconds).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import requests

from seed_ub_candidate_ips import ALL_CANDIDATES

BGG_SEARCH_URL = "https://boardgamegeek.com/xmlapi2/search"
BGG_THING_URL = "https://boardgamegeek.com/xmlapi2/thing"
RATE_LIMIT_SEC = 2.0

# BGG's xmlapi2 now requires authentication. Token is shared with
# harvesters/bgg_harvester.py — keep them in sync if BGG rotates it.
BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"
BGG_HEADERS = {"Authorization": f"Bearer {BGG_TOKEN}"}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


@dataclass
class SearchHit:
    bgg_id: int
    name: str
    year: int | None
    owned_count: int = 0  # filled in by stats lookup


def search_bgg(query: str) -> list[SearchHit]:
    """Search BGG for the query, return up to 5 most plausible hits."""
    url = (
        f"{BGG_SEARCH_URL}?query={requests.utils.quote(query)}&type=boardgame"
    )
    try:
        resp = requests.get(url, headers=BGG_HEADERS, timeout=20)
        if resp.status_code != 200:
            log.warning("Search %r returned %d", query, resp.status_code)
            return []
        root = ET.fromstring(resp.text)
        hits: list[SearchHit] = []
        for item in root.findall("item")[:8]:
            try:
                bgg_id = int(item.attrib["id"])
                name_node = item.find("name")
                year_node = item.find("yearpublished")
                if name_node is None:
                    continue
                hits.append(
                    SearchHit(
                        bgg_id=bgg_id,
                        name=name_node.attrib.get("value", ""),
                        year=(
                            int(year_node.attrib["value"])
                            if year_node is not None
                            else None
                        ),
                    )
                )
            except (KeyError, ValueError):
                continue
        return hits
    except Exception as e:
        log.warning("Search failed for %r: %s", query, e)
        return []


def fetch_owned_counts_batch(bgg_ids: list[int]) -> dict[int, int]:
    """Fetch owned_count for many BGG IDs in ONE call.

    BGG's xmlapi2/thing endpoint accepts comma-separated IDs and
    returns one <item> per ID. Costs one API call regardless of how
    many IDs you batch — typical batch is 5-8 hits per IP search.
    """
    if not bgg_ids:
        return {}
    id_str = ",".join(str(i) for i in bgg_ids)
    url = f"{BGG_THING_URL}?id={id_str}&stats=1"
    try:
        resp = requests.get(url, headers=BGG_HEADERS, timeout=30)
        if resp.status_code == 202:
            time.sleep(5)
            return fetch_owned_counts_batch(bgg_ids)
        if resp.status_code != 200:
            return {bid: 0 for bid in bgg_ids}
        root = ET.fromstring(resp.text)
        out: dict[int, int] = {}
        for item in root.findall("item"):
            try:
                bid = int(item.attrib["id"])
                owned = item.find("statistics/ratings/owned")
                out[bid] = int(owned.attrib.get("value", 0)) if owned is not None else 0
            except (KeyError, ValueError):
                continue
        # Fill any missing IDs with 0 (BGG sometimes omits items)
        for bid in bgg_ids:
            out.setdefault(bid, 0)
        return out
    except Exception as e:
        log.warning("Batch stats fetch failed: %s", e)
        return {bid: 0 for bid in bgg_ids}


def find_ip_matches(ip_name: str, top_k: int = 3) -> list[dict]:
    """Search BGG for an IP and return top_k candidate matches by ownership.

    Two API calls per IP: one search + one batched stats fetch. Total
    ~4s of wall-clock per IP after rate limiting (vs. ~16s in the
    per-hit version).
    """
    hits = search_bgg(ip_name)
    time.sleep(RATE_LIMIT_SEC)
    if not hits:
        return []

    # ONE batched stats call covers all the hits for this IP.
    owned_by_id = fetch_owned_counts_batch([h.bgg_id for h in hits])
    time.sleep(RATE_LIMIT_SEC)
    for h in hits:
        h.owned_count = owned_by_id.get(h.bgg_id, 0)

    hits.sort(key=lambda h: h.owned_count, reverse=True)
    return [
        {
            "bgg_id": h.bgg_id,
            "name": h.name,
            "year": h.year,
            "owned_count": h.owned_count,
        }
        for h in hits[:top_k]
        if h.owned_count > 0
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--names",
        help="Comma-separated IP names to query (case-sensitive, must "
             "match seed list exactly). Use this to query only the "
             "uncertain entries when filling gaps in hand-curation. "
             "Overrides --limit.",
    )
    parser.add_argument(
        "--output", default="scripts/_ub_bgg_search_results.json",
        help="Where to write the proposal JSON (review material).",
    )
    parser.add_argument(
        "--top-k", type=int, default=3,
        help="Top N matches to keep per IP.",
    )
    args = parser.parse_args()

    if args.names:
        wanted = {n.strip() for n in args.names.split(",")}
        candidates = [c for c in ALL_CANDIDATES if c.name in wanted]
        missing = wanted - {c.name for c in candidates}
        if missing:
            log.warning(
                "Names not found in seed list: %s", sorted(missing)
            )
    else:
        candidates = list(ALL_CANDIDATES)
        if args.limit:
            candidates = candidates[: args.limit]

    log.info("Searching BGG for %d UB candidate IPs...", len(candidates))
    log.info(
        "Estimated runtime: ~%.1f min (%d IPs × ~4s each)",
        len(candidates) * 4 / 60,
        len(candidates),
    )

    results: dict[str, list[dict]] = {}
    for i, ip in enumerate(candidates, 1):
        log.info("[%d/%d] %s (%s)", i, len(candidates), ip.name, ip.medium)
        matches = find_ip_matches(ip.name, top_k=args.top_k)
        if matches:
            results[ip.name] = matches
            for m in matches:
                log.info(
                    "    %d  owned=%d  '%s' (%s)",
                    m["bgg_id"], m["owned_count"], m["name"], m["year"],
                )
        else:
            log.info("    (no matches)")

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    log.info(
        "Wrote %d IP -> matches mappings to %s", len(results), args.output
    )
    log.info(
        "%d IPs had no BGG hits with owned_count > 0",
        len(candidates) - len(results),
    )


if __name__ == "__main__":
    main()
