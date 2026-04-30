"""
Arcane Analytics — forum thread body scraper for UB IPs
(Stage 7a-ii of community_reception, Apr 30 2026).

Stage 7a-i classified each forum top URL using only title+snippet from
the Google CSE harvest — the cheap path. That input is too thin to
detect backlash narrative rhetoric: Stage 7c showed only 5 backlash
narratives across 218 classified threads, and Gemini repeatedly
abstained on subtle fit-evaluation language because it didn't have
enough thread text to ground a label.

This script Playwrights each top-thread URL, extracts the OP + first
~20 replies, and stores them in `forum_thread_bodies`. Once that
table is populated, classify_forum_top_urls.py and
classify_forum_narratives.py rerun with body text appended to the
existing title+snippet input — the higher-resolution Stage 7c v2.

Per-forum selector strategy:

    enworld.org           XenForo 2: article.message + .bbWrapper
    rpg.net               XenForo 2: same; Cloudflare risk flagged
    forums.giantitp.com   vBulletin: div[id^="post_message_"]
    dragonsfoot.org       phpBB:    .postbody .content

Operational notes:
- Anonymous browser sessions (no login). All 704 URLs are CSE-indexed,
  meaning publicly readable, so account login wouldn't unlock content
  and would put the user's account at risk under TOS.
- 5 sec/URL pacing with ±1s jitter; halts a forum's queue on first 429.
- Idempotent: skips URLs already present in forum_thread_bodies (LEFT
  JOIN check at startup).
- Cloudflare interstitial detection: if HTTP 403 or challenge HTML
  detected, log + skip + flag scrape_status='cloudflare_blocked'. RPG.net
  bookmarklet fallback (Stage 7b) handles those.

Usage:

    python scripts/scrape_forum_thread_bodies.py --smoke-test
        Fetch ONE URL per forum, print extraction results, no BQ writes.

    python scripts/scrape_forum_thread_bodies.py --forum=forums.giantitp.com
        Scrape one forum at a time. Recommended for initial bulk runs.

    python scripts/scrape_forum_thread_bodies.py --all
        Scrape all 704 URLs. ~60 min runtime expected.

Auth:
- BigQuery: ADC. No external secrets needed for Playwright.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import random
import re
from dataclasses import dataclass

from google.cloud import bigquery
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

PROJECT_ID = "dnd-trends-index"
PRESENCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.forum_presence_counts"
BODIES_TABLE = f"{PROJECT_ID}.dnd_trends_raw.forum_thread_bodies"

# Forums whose Cloudflare config blocks Playwright + curl_cffi entirely
# (verified Apr 30 2026: 4-profile curl_cffi TLS impersonation + vanilla
# headless chromium both 403 with cf_browser_verification interstitial).
# Skipped in bulk runs; --forum=<domain> override bypasses this list.
# GitP slice is queued for a Stage 7b bookmarklet pass that runs in
# Phil's authenticated browser session (DDB-pattern same-origin fetch).
CLOUDFLARE_BLOCKED_FORUMS = {"forums.giantitp.com", "dragonsfoot.org"}

# Replies depth + per-post truncation. OP + first 20 replies, each
# ≤2000 chars. Total payload bound: ~42 KB/thread, ~30 MB across 704
# threads. Well within BQ row size limits and Gemini Flash context.
MAX_REPLIES = 20
MAX_CHARS_PER_POST = 2000

# Pacing (per forum). 5s mean + ±1s jitter ≈ moderate-human cadence.
RATE_LIMIT_SEC = 5.0
RATE_LIMIT_JITTER = 1.0

# Cloudflare interstitial markers: anything matching these in HTML or
# title means we hit the "Just a moment..." gate, not the actual thread.
CLOUDFLARE_MARKERS = [
    "Just a moment",
    "cf-browser-verification",
    "cf-challenge-running",
    "Attention Required! | Cloudflare",
]

# Realistic User-Agent strings — rotated per page.goto so requests
# don't all share a fingerprint. Kept narrow to common 2025 browsers.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

# Per-forum extraction config. Different forum software → different
# DOM. Each entry has the post-container selector plus the inner body
# selector (relative to the container). The smoke test will validate
# these selectors on real URLs before we commit to a bulk run.
FORUM_CONFIG = {
    "enworld.org": {
        "software": "XenForo 2",
        "post_container": "article.message",
        "post_body": ".bbWrapper",
        # XenForo wraps the post header in `.message-cell--user`; the
        # bbWrapper inside `.message-body` is the prose. We'll fall
        # back to .message-body if .bbWrapper misses.
        "post_body_fallback": ".message-body",
    },
    "rpg.net": {
        "software": "XenForo 2",
        "post_container": "article.message",
        "post_body": ".bbWrapper",
        "post_body_fallback": ".message-body",
    },
    "forums.giantitp.com": {
        "software": "vBulletin",
        # GitP runs vB4. Posts live in div[id^="post_message_"].
        "post_container": "div[id^='post_message_']",
        "post_body": None,  # the container IS the body
        "post_body_fallback": None,
    },
    "dragonsfoot.org": {
        "software": "phpBB",
        # Dragonsfoot is phpBB3. Each post is .post containing
        # .postbody. Body prose is in .content.
        "post_container": ".post",
        "post_body": ".postbody .content",
        "post_body_fallback": ".content",
    },
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


@dataclass
class ScrapeResult:
    ip_name: str
    url: str
    forum_domain: str
    op_text: str
    replies_text_combined: str
    reply_count: int
    scrape_status: str  # success | cloudflare_blocked | not_found | other_error
    error_message: str
    scraped_at: str


# ---------- helpers ----------

def normalize_text(text: str) -> str:
    """Collapse whitespace, strip, truncate."""
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text[:MAX_CHARS_PER_POST]


def looks_like_cloudflare(html: str, title: str) -> bool:
    blob = f"{title}\n{html[:2000]}"  # only check head of HTML
    return any(m.lower() in blob.lower() for m in CLOUDFLARE_MARKERS)


async def extract_posts(page: Page, forum_domain: str) -> tuple[str, list[str]]:
    """Extract OP text + reply texts using per-forum selectors.

    Returns (op_text, replies_texts). replies_texts may be empty if the
    thread has no replies yet.
    """
    cfg = FORUM_CONFIG[forum_domain]
    container_sel = cfg["post_container"]
    body_sel = cfg["post_body"]
    body_fallback = cfg["post_body_fallback"]

    # Wait for at least one post container to appear. Generous timeout
    # — this is also what gives Cloudflare's JS challenge time to clear
    # before we conclude "selector miss". If the page is still on a
    # Cloudflare interstitial after 15s, the container will never appear
    # and we surface that as "other_error".
    try:
        await page.wait_for_selector(container_sel, timeout=15000)
    except PlaywrightTimeout:
        return "", []

    containers = await page.query_selector_all(container_sel)
    if not containers:
        return "", []

    texts = []
    for c in containers[: MAX_REPLIES + 1]:
        # Prefer body_sel; fall back; last resort, use the container itself.
        node = None
        if body_sel:
            node = await c.query_selector(body_sel)
            if not node and body_fallback:
                node = await c.query_selector(body_fallback)
        if not node:
            node = c
        try:
            t = await node.inner_text()
        except Exception:
            t = ""
        t = normalize_text(t)
        if t:
            texts.append(t)

    if not texts:
        return "", []

    op = texts[0]
    replies = texts[1 : MAX_REPLIES + 1]
    return op, replies


async def scrape_one(
    page: Page, ip_name: str, url: str, forum_domain: str
) -> ScrapeResult:
    """Fetch + extract one thread URL. Catches all exceptions and
    classifies them into scrape_status values."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    base = ScrapeResult(
        ip_name=ip_name,
        url=url,
        forum_domain=forum_domain,
        op_text="",
        replies_text_combined="",
        reply_count=0,
        scrape_status="other_error",
        error_message="",
        scraped_at=now,
    )

    try:
        # domcontentloaded fires fast; we wait for the post-container
        # selector below to know the real DOM is rendered. networkidle
        # never fires on modern forums (live-polling, ads, analytics).
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PlaywrightTimeout as e:
        base.scrape_status = "other_error"
        base.error_message = f"goto timeout: {e}"
        return base
    except Exception as e:
        base.scrape_status = "other_error"
        base.error_message = f"goto error: {e}"
        return base

    if response is None:
        base.scrape_status = "other_error"
        base.error_message = "no response"
        return base

    status = response.status

    if status == 404:
        base.scrape_status = "not_found"
        return base

    if status == 429:
        base.scrape_status = "rate_limited"
        base.error_message = "HTTP 429"
        return base

    if status == 403:
        base.scrape_status = "cloudflare_blocked"
        base.error_message = "HTTP 403"
        return base

    if status >= 400:
        base.scrape_status = "other_error"
        base.error_message = f"HTTP {status}"
        return base

    # Cloudflare can return 200 with an interstitial. Sniff title + HTML.
    try:
        title = await page.title()
        # Tiny snippet of HTML for sniffing — full content() is slow.
        html_head = await page.evaluate(
            "() => document.documentElement.outerHTML.slice(0, 2000)"
        )
    except Exception:
        title, html_head = "", ""

    if looks_like_cloudflare(html_head, title):
        base.scrape_status = "cloudflare_blocked"
        base.error_message = "Cloudflare interstitial"
        return base

    # Selectors-based extraction.
    try:
        op, replies = await extract_posts(page, forum_domain)
    except Exception as e:
        base.scrape_status = "other_error"
        base.error_message = f"extract error: {e}"
        return base

    if not op:
        # Selector miss — important to flag, don't silently mark success.
        base.scrape_status = "other_error"
        base.error_message = "selector miss (no OP found)"
        return base

    base.op_text = op
    base.replies_text_combined = "\n\n---\n\n".join(replies)
    base.reply_count = len(replies)
    base.scrape_status = "success"
    return base


# ---------- BQ helpers ----------

def load_pending_urls(
    bq: bigquery.Client, forum_filter: str | None, limit: int | None
) -> list[tuple[str, str, str]]:
    """Return (ip_name, url, forum_domain) for URLs not yet in
    forum_thread_bodies. Skips Cloudflare-blocked forums by default
    unless --forum override targets them explicitly."""
    if forum_filter:
        where_forum = f"AND t.forum_domain = '{forum_filter}'"
    else:
        skip = ", ".join(f"'{f}'" for f in CLOUDFLARE_BLOCKED_FORUMS)
        where_forum = f"AND t.forum_domain NOT IN ({skip})"
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    q = f"""
    WITH unnested AS (
      SELECT p.ip_name, t.url, t.forum_domain
      FROM `{PRESENCE_TABLE}` p, UNNEST(p.top_thread_urls) AS t
      WHERE t.forum_domain IN ('enworld.org','rpg.net',
                               'forums.giantitp.com','dragonsfoot.org')
        AND NOT REGEXP_CONTAINS(t.url, r'archive/index\\.php')
        {where_forum}
    )
    SELECT u.ip_name, u.url, u.forum_domain
    FROM unnested u
    LEFT JOIN `{BODIES_TABLE}` b USING (url)
    WHERE b.url IS NULL
    GROUP BY u.ip_name, u.url, u.forum_domain
    ORDER BY u.forum_domain, u.ip_name
    {limit_clause}
    """
    return [(r["ip_name"], r["url"], r["forum_domain"]) for r in bq.query(q).result()]


def insert_results(bq: bigquery.Client, rows: list[ScrapeResult]) -> None:
    if not rows:
        return
    payload = [
        {
            "ip_name": r.ip_name,
            "url": r.url,
            "forum_domain": r.forum_domain,
            "op_text": r.op_text,
            "replies_text_combined": r.replies_text_combined,
            "reply_count": r.reply_count,
            "scrape_status": r.scrape_status,
            "error_message": r.error_message,
            "scraped_at": r.scraped_at,
        }
        for r in rows
    ]
    errors = bq.insert_rows_json(BODIES_TABLE, payload)
    if errors:
        log.error("BQ insert errors: %s", errors)
        raise RuntimeError("insert_rows_json failed")


# ---------- modes ----------

async def smoke_test() -> None:
    """One URL per forum. Print extracted content for selector validation.
    No BQ writes."""
    bq = bigquery.Client(project=PROJECT_ID)
    # Exclude vBulletin archive URLs — they're a stripped read-only view
    # without normal post containers, useless for sentiment classification
    # and they account for ~50% of GitP harvest URLs from CSE.
    q = f"""
    SELECT t.forum_domain, t.url, p.ip_name
    FROM `{PRESENCE_TABLE}` p, UNNEST(p.top_thread_urls) AS t
    WHERE t.forum_domain IN ('enworld.org','rpg.net',
                             'forums.giantitp.com','dragonsfoot.org')
      AND NOT REGEXP_CONTAINS(t.url, r'archive/index\\.php')
    QUALIFY ROW_NUMBER() OVER
      (PARTITION BY t.forum_domain ORDER BY p.harvested_at DESC) = 1
    ORDER BY t.forum_domain
    """
    samples = list(bq.query(q).result())
    log.info("Smoke-testing %d sample URLs (one per forum):", len(samples))
    for s in samples:
        log.info("  %-22s %s", s["forum_domain"], s["url"])

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENTS[0],
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        for s in samples:
            forum = s["forum_domain"]
            url = s["url"]
            ip = s["ip_name"]
            log.info("=" * 70)
            log.info("FORUM: %s  (%s)", forum, FORUM_CONFIG[forum]["software"])
            log.info("URL:   %s", url)
            log.info("IP:    %s", ip)

            result = await scrape_one(page, ip, url, forum)

            log.info("STATUS: %s", result.scrape_status)
            if result.error_message:
                log.info("ERROR:  %s", result.error_message)
            log.info("REPLIES EXTRACTED: %d", result.reply_count)
            log.info("--- OP TEXT (first 600 chars) ---")
            log.info(result.op_text[:600] or "(empty)")
            if result.replies_text_combined:
                log.info("--- FIRST REPLY (first 400 chars) ---")
                first_reply = result.replies_text_combined.split("\n\n---\n\n")[0]
                log.info(first_reply[:400])

            await asyncio.sleep(RATE_LIMIT_SEC)

        await browser.close()


async def bulk_run(forum_filter: str | None, limit: int | None,
                   dry_run: bool) -> None:
    """Scrape all pending URLs (or filtered subset). Writes to BQ."""
    bq = bigquery.Client(project=PROJECT_ID)
    pending = load_pending_urls(bq, forum_filter, limit)
    log.info("%d URLs pending scrape%s.", len(pending),
             f" [forum={forum_filter}]" if forum_filter else "")
    if dry_run:
        log.info("DRY-RUN: not scraping.")
        return
    if not pending:
        log.info("Nothing to do.")
        return

    # Stop-on-429 tracking per forum
    halted_forums: set[str] = set()
    results: list[ScrapeResult] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENTS[0],
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        for i, (ip, url, forum) in enumerate(pending, 1):
            if forum in halted_forums:
                log.info("[%d/%d] SKIP (forum halted): %s", i, len(pending), url)
                continue
            log.info("[%d/%d] %-22s %s", i, len(pending), forum, url)
            result = await scrape_one(page, ip, url, forum)
            log.info("    -> %s (replies=%d)",
                     result.scrape_status, result.reply_count)
            results.append(result)

            if result.scrape_status == "rate_limited":
                log.warning("    Halting forum %s on 429.", forum)
                halted_forums.add(forum)

            # Flush every 25 rows so a crash doesn't lose everything.
            if len(results) >= 25:
                insert_results(bq, results)
                log.info("    Flushed %d rows to BQ.", len(results))
                results = []

            jitter = random.uniform(-RATE_LIMIT_JITTER, RATE_LIMIT_JITTER)
            await asyncio.sleep(max(0.5, RATE_LIMIT_SEC + jitter))

        await browser.close()

    if results:
        insert_results(bq, results)
        log.info("Flushed final %d rows to BQ.", len(results))

    log.info("=" * 60)
    log.info("Bulk scrape done.")


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--smoke-test", action="store_true",
                      help="Fetch one URL per forum, print results, no writes.")
    mode.add_argument("--all", action="store_true",
                      help="Scrape all pending URLs across all forums.")
    mode.add_argument("--forum", help="Scrape only one forum_domain.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N URLs (use for staged runs).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Load pending URLs but don't scrape.")
    args = parser.parse_args()

    if args.smoke_test:
        asyncio.run(smoke_test())
    else:
        asyncio.run(bulk_run(args.forum, args.limit, args.dry_run))


if __name__ == "__main__":
    main()
