# Data Streams Health Report

**Date:** 2026-08-31
**Method:** Static code + git-history audit (see "What I could NOT verify" below — this is not a live BigQuery pull)

---

## TL;DR

- The pipeline runs (or is meant to run) **~23 data streams** across 5 signal families (Curiosity, Community, Creator, Ownership, Commerce) plus one macro/context stream (Freight Index). The README only documents 8 of these — the system has grown well past its own docs.
- **Confirmed, matching what you told me:** Amazon, Kickstarter, BackerKit, DMs Guild, and DriveThruRPG are all human-bookmarklet-fed with zero Cloud Scheduler behind them. They're behind because someone has to physically click the bookmarklet — that's expected, not broken.
- **New finding:** 6 fully-built Cloud Functions — **Steam, mod.io, Nexus Mods, Twitch, AO3, D&D Beyond Catalog** — have no scheduler job, cron string, or deploy script anywhere in the repo. They may be wired up outside version control, or they may not be running on any cadence at all. One `gcloud scheduler jobs list` would settle it.
- **New finding:** Itch.io Jams has no ingestion-date column at all — there's genuinely no way to tell "stale" from "no new jams this week" for that table today.
- **New finding:** Freight Index's insert has no dedup guard (a manual re-trigger double-inserts), and its documented cron disagrees with its deployed cron in a way that may drift into the Shabbat blackout window in summer.
- I extended `stream_health_audit.py` from 8 streams to all ~23 so the next live run gives full coverage, with realistic staleness thresholds for manual vs. automated vs. cadence-unknown streams. I could not run it — see below.

---

## What I could NOT verify (read this first)

This session is a sandboxed remote container with:
- No `dnd-key.json` / `GOOGLE_APPLICATION_CREDENTIALS` — can't authenticate a BigQuery client.
- No `gcloud` or `bq` CLI installed.
- Outbound network policy blocks `*.cloudfunctions.net` and `*.run.app` (confirmed: `curl` to the live Bouncer API's `/system/health` endpoint returned a 403 at the proxy level, not from the API itself).

So **every "Rows" / "Latest Data" / "Days Stale" number a live run would produce is absent here.** Worth knowing while you're looking at this: the Bouncer's `/system/health` route (`bouncer/main.py:650-737`) already runs a real per-table freshness/row-count query against BigQuery's `__TABLES__` metadata for 14 sources — it is **not** the mocked stub the old `context_docs/CONTEXT.md` (last updated March) describes. Only the `caldean_cycle` block in that same response is still hardcoded/mocked. If you hit that URL yourself (or from an environment that isn't network-blocked the way this one is), you'll get real numbers today.

Everything below is what the **code itself** — table schemas, dedup guards, scheduler configs, git history, and your own team's comments — says about each stream's design and known failure history.

---

## Stream Inventory

Legend: **Trigger** = how the code says it runs. "UNVERIFIED" means the Cloud Function exists and is wired to write a real table, but no scheduler artifact for it exists anywhere in the repo.

### Curiosity — "Are people searching for this?"

| Stream | Table | Trigger | Known health signal |
|---|---|---|---|
| Google Trends | `dnd_trends_categorized.trend_data_pilot` | Cloud Run Job via `dnd-fast-lane` workflow | Circuit breaker + fail-closed proxy guard added after a 3.2GB bandwidth burn (37min × 6 retries); scrape-queue dedup landed 2026-06-09 (~10% bandwidth saved) |
| Wikipedia | `social_data.wikipedia_daily_views` | Cloud Run service via `dnd-fast-lane` | Was the **pilot stream** for the new fixed-time scheduling policy; a watermark `None`-crash that had silently killed the stream was fixed 2026-05-19 |
| Twitch | `dnd_trends_raw.twitch_viewership` | HTTP Cloud Function — **UNVERIFIED cadence** | No scheduler artifact found in repo |

### Community — "Are people talking about this?"

| Stream | Table | Trigger | Known health signal |
|---|---|---|---|
| Reddit | `dnd_trends_categorized.reddit_daily_metrics` | Cloud Run service via `dnd-fast-lane` | — |
| Fandom | `dnd_trends_raw.fandom_daily_metrics` | Cloud Run service via `dnd-fast-lane` | **Repointed 2026-05-19** — the audit was watching a dead legacy table (`social_data.fandom_trending`, no writes since Jan 1); this is the live target |

### Creator — "Are people making content about this?"

| Stream | Table | Trigger | Known health signal |
|---|---|---|---|
| YouTube | `dnd_trends_raw.youtube_videos` | Cloud Run service via `dnd-fast-lane` | No ingestion-timestamp column exists — `published_at` is the video's publish date, not scrape date. Freshness genuinely can't be measured; open TODO |
| Itch.io Products | `dnd_trends_raw.itchio_products` | Cloud Run Job via `dnd-fast-lane` | Dedup is by `product_id`, not date-scoped — a broken RSS feed silently inserts 0 rows with no error surfaced |
| Itch.io Jams | `dnd_trends_raw.itchio_jams` | Cloud Run Job via `dnd-fast-lane` | **No ingestion-date column at all.** `start_date`/`end_date` are the jam's own content dates. Same silent-empty-feed risk as Products |
| mod.io | `dnd_trends_raw.modio_mods` | HTTP Cloud Function — **UNVERIFIED cadence** | No scheduler artifact found in repo |
| Nexus Mods | `dnd_trends_raw.nexus_mods` | HTTP Cloud Function — **UNVERIFIED cadence** | No scheduler artifact found in repo |
| AO3 | `dnd_trends_raw.ao3_tag_counts` | HTTP Cloud Function — **UNVERIFIED cadence** | No scheduler artifact found in repo |

### Ownership — "Are people playing/owning this?"

| Stream | Table | Trigger | Known health signal |
|---|---|---|---|
| BGG | `dnd_trends_raw.bgg_product_stats` | HTTP Cloud Function via `dnd-fast-lane` (`{"rpg": false}`) | Has a real date-scoped dedup guard (safe to rerun same day). BGG started blocking GCP datacenter IPs; fixed with fail-closed proxy routing 2026-06-07 |
| RPGGeek | `dnd_trends_raw.rpggeek_product_stats` | Same function as BGG (`{"rpg": true}`) | Same as BGG |
| Roll20 | `commercial_data.roll20_rankings` | **unknown** — not present in `dnd-fast-lane.yaml`, no scheduler artifact found | — |
| Steam | `dnd_trends_raw.steam_player_counts` | HTTP Cloud Function — **UNVERIFIED cadence** | No dedup guard — repeat same-day runs create duplicate `snapshot_date` rows (inflates counts, doesn't break freshness) |

### Commerce — "Are people buying this?"

| Stream | Table | Trigger | Known health signal |
|---|---|---|---|
| Kickstarter | `commercial_data.kickstarter_projects` | **MANUAL** — browser bookmarklet, no scheduler | On-demand by design |
| BackerKit | `commercial_data.backerkit_projects` | **MANUAL** — bookmarklet since 2026-05-18 | Server-side Cloud Function is dead code: deterministic 403 from BackerKit's WAF confirmed persistent on every scheduled run since ≥2026-05-13 (GCP datacenter IPs flagged) |
| Amazon | `dnd_trends_raw.amazon_daily_stats` | **MANUAL** — bookmarklet → `bouncer /system/amazon/ingest-ranks` | Has a real date-scoped dedup guard (safe to rerun same day) |
| DMs Guild | `dnd_trends_raw.catalog_supply` (`source='DMs Guild'`) | **MANUAL** — "Arcane Incursion" bookmarklet/Chrome extension (Mon 6am + daily retry, skip Sat) but only fires if that browser+extension is actually open | Programmatic Playwright scraper was attempted and abandoned — Cloudflare Turnstile + JA3 TLS fingerprinting defeated 3 stealth configurations (`dtrpg_scraping_report.md`) |
| DriveThruRPG | `dnd_trends_raw.catalog_supply` (`source='DriveThruRPG'`) | Same manual path as DMs Guild, same table | No dedup guard on the ingest-catalog route — repeat bookmarklet runs append duplicate rows |
| D&D Beyond Catalog | `dnd_trends_raw.dndbeyond_catalog` | HTTP Cloud Function — **UNVERIFIED cadence** | Product discovery regex-scrapes React hydration JSON off the DDB marketplace page — fragile to any frontend change, no test coverage |

### Context / macro (outside the 5-family model)

| Stream | Table | Trigger | Known health signal |
|---|---|---|---|
| Freight Index | `gold_data.freight_index_daily` | Dedicated weekly Cloud Scheduler (Sat 22:00 CST), standalone — not in `dnd-fast-lane` | Feeds "Quartermaster" shipping-cost articles. **No dedup guard** — a manual re-trigger same day double-inserts. The docstring's cron comment (`0 3 * * 0` UTC) disagrees with the deployed Sat-22:00-CST cron — in summer CDT that's ~03:00 UTC, inside the Shabbat blackout window that's supposed to end at 03:45 UTC per `context_docs/SCHEDULING.md`. Worth a look. |
| Emerging Terms (discovery) | `dnd_trends_raw.emerging_terms` | Cloud Scheduler `discover-related-queries-weekly`, deliberately managed out-of-band | Proxy was silently falling back to unauthenticated (wrong env var in `deploy.sh`) — fixed 2026-06-09; runs before that date may have been failing quietly |

---

## Two things worth a decision regardless of live numbers

1. **Six built Cloud Functions have no scheduler evidence** (Steam, mod.io, Nexus, Twitch, AO3, D&D Beyond Catalog). Either they're triggered via `gcloud` outside version control, or `gold_views/digital_streams_health.sql`'s "latest" numbers for these six could be arbitrarily old without anyone noticing — that view only checks "what's the newest row," it doesn't flag staleness the way `stream_health_audit.py` does. `gcloud scheduler jobs list --location=us-central1` resolves this in one command.
2. **Freight Index has no dedup guard and a cron that may drift into the Shabbat blackout** in summer CDT, despite the docstring claiming it's idempotent. Small fix (a `WHERE NOT EXISTS` guard + pinning the cron to an explicit UTC time well clear of 21:30–03:45 UTC) next time someone's in that file.

Neither of these needed live data to find — they're structural.

---

## What changed in this branch

- `stream_health_audit.py`: extended from 8 streams to all ~23 identified above, with per-stream `trigger` metadata and realistic staleness thresholds — manual/bookmarklet streams get a 30-day threshold instead of the default 4 (so "haven't scraped Amazon this week" stops reading as a false alarm), and cadence-unknown Cloud Functions get 14 days instead of silently inheriting a threshold that assumes a schedule that may not exist.
- Nothing else touched. No BigQuery writes, no deploys, no scheduler changes — none of those are possible from this session anyway (no credentials), and CLAUDE.md gates them regardless.

## Next step to get real numbers

Run `python3 stream_health_audit.py` anywhere with BigQuery read access (your devcontainer with `dnd-key.json`, or Cloud Shell), or hit the live Bouncer `/system/health` endpoint directly from a browser or a non-sandboxed shell. Paste the output back and I'll turn it into the live version of this report — actual row counts, actual days-stale, actual red/green per stream.
