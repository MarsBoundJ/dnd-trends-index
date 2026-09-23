# Data Streams Health Report

**Date:** 2026-08-31 (updated same day with live data once credentials were granted)
**Method:** Originally a static code + git-history audit (kept below for the "why" behind each stream). A `claude-code` service account was granted BigQuery/Scheduler/Logging read access later the same day, so the sections below now include a real `stream_health_audit.py` run plus a live Cloud Scheduler + Cloud Logging investigation.

---

## LIVE RESULTS (2026-08-31, run with real credentials)

Full `stream_health_audit.py` output against production BigQuery. 7 of 24 streams are STALE:

| Stream | Status | Rows | Latest Data | Days Stale | Threshold |
|---|---|---|---|---|---|
| Google Trends | Healthy | 1,258,134 | 2026-08-31 | 0 | 4 |
| Wikipedia | Healthy | 983,847 | 2026-08-30 | 1 | 3 |
| Twitch | Healthy | 3,610 | 2026-08-31 | 0 | 14 |
| **Emerging Terms** | **STALE** | 116 | 2026-04-12 | **141** | 9 |
| Reddit | Healthy | 20,420 | 2026-08-31 | 0 | 3 |
| Fandom | Healthy | 383,156 | 2026-08-31 | 0 | 4 |
| YouTube | Healthy (no freshness check) | 3,213 | 2026-05-31 (content date) | n/a | n/a |
| Itch.io Products | Healthy | 170 | 2026-08-31 | 0 | 5 |
| Itch.io Jams | Healthy (no freshness check) | 2,060 | n/a (content date) | n/a | n/a |
| mod.io | Healthy | 12,100 | 2026-08-31 | 0 | 14 |
| Nexus Mods | Healthy | 3,242 | 2026-08-31 | 0 | 14 |
| AO3 | Healthy | 390 | 2026-08-30 | 1 | 14 |
| BGG | Healthy | 3,398 | 2026-08-31 | 0 | 4 |
| **RPGGeek** | **STALE** | 45 | 2026-03-30 | **154** | 4 |
| Roll20 | Healthy | 13,400 | 2026-08-31 | 0 | 4 |
| Steam | Healthy | 23,478 | 2026-08-31 | 0 | 14 |
| **Kickstarter** | **STALE** | 1,485 | 2026-05-18 | **105** | 30 |
| **BackerKit** | **STALE** | 378 | 2026-05-19 | **104** | 30 |
| **Amazon** | **STALE** | 2,319 | 2026-05-18 | **105** | 30 |
| **DMs Guild** | **STALE** | 22,374 (shared table) | 2026-05-19 | **104** | 30 |
| **DriveThruRPG** | **STALE** | 22,374 (shared table) | 2026-05-19 | **104** | 30 |
| D&D Beyond Catalog | Healthy | 417 | 2026-08-31 | 0 | 14 |
| Freight Index | Healthy (close to threshold) | 60 | 2026-08-23 | 8 | 9 |
| Daily Articles (AI) | Healthy | 293 | — | — | — |

**The 5 manual streams are exactly what you told me at the start** — Amazon, Kickstarter, BackerKit, DMs Guild, DriveThruRPG all stopped the moment the bookmarklets stopped being run, right around 2026-05-18/19. Expected, not broken.

**The "6 unverified cadence" concern from the static audit is now closed.** `gcloud`-equivalent scheduler listing (via the Python client) shows all of them — Steam, mod.io, Nexus, Twitch, AO3, D&D Beyond Catalog — have real, enabled Cloud Scheduler jobs, following a consistent pattern: a `-shabbat`-suffixed weekday job plus a `-motzei-shabbat` Saturday-night catch-up job. They were just never captured in the repo (created directly via `gcloud`/Console). No action needed there beyond, optionally, documenting them in version control.

### New finding #1 — RPGGeek: root-caused, one-line fix

`rpggeek-harvester-schedule` fires correctly on schedule (confirmed: last fired today, 2026-08-31 03:15 UTC, right after `bgg-harvester-schedule` at 03:00). But 2 weeks of Cloud Run logs for the `bgg-harvester` service show **every single invocation — both the BGG and RPGGeek triggers — logs `RPG: False`.** The RPGGeek path has never actually executed.

Root cause: the Cloud Scheduler job's HTTP target sends `Content-Type: application/octet-stream` instead of `application/json` (confirmed on both `bgg-harvester-schedule` and `rpggeek-harvester-schedule`). `cloud_functions/bgg_harvester/main.py:76` calls `request.get_json(silent=True)`, which Flask silently returns `None` for when the Content-Type isn't `application/json` — so `data` falls back to `{}` and `is_rpg = data.get("rpg", False)` is always `False`, regardless of the `{"rpg":true}` body Cloud Scheduler is actually sending. The RPGGeek job has therefore been a wasted duplicate BGG no-op (it hits the "data already exists for today" dedup check right after BGG's own run and skips) since whenever this Content-Type misconfiguration was introduced — at least back to 2026-03-30, the last date RPGGeek data actually landed.

**Proposed fix (pending your go-ahead — this is a `gcloud`/deploy action, gated by your own CLAUDE.md):**
1. Code fix: change `request.get_json(silent=True)` to `request.get_json(silent=True, force=True)` in `cloud_functions/bgg_harvester/main.py` — makes the function robust to Content-Type regardless of how it's invoked (defense in depth).
2. Infra fix: update `rpggeek-harvester-schedule`'s (and `bgg-harvester-schedule`'s, for consistency) Content-Type header to `application/json` via `gcloud scheduler jobs update http ...`.
3. Redeploy `bgg-harvester` Cloud Run service with the code fix.

### New finding #2 — Emerging Terms: root-caused, not a quick fix

`discover-related-queries-weekly` also fires correctly (confirmed: today, 06:00 UTC). Logs show it runs to completion without crashing, but **every seed keyword's `related_queries()`/`related_topics()` pytrends call is being rate-limited by Google Trends** — the first seed gets two `429 Too Many Requests` responses and is skipped; every subsequent seed's `related_topics()` returns an empty result set (Google's soft-block behavior — no error, just nothing). Net effect: the function has been running on schedule since April but discovering 0 new terms every time.

This is the same class of problem your team already fought for the main Google Trends scraper and BGG (aggressive rate-limiting on pytrends specifically) — not a one-line fix. It likely needs the same treatment those got: better proxy rotation/backoff tuned for this specific endpoint, spacing requests out further, or accepting a lower per-run success rate. Flagging it root-caused rather than proposing a fix outright, since this deserves a real decision on approach rather than a quick patch.

### Still valid from the original audit — Freight Index

Confirmed live: 8 days stale against a 9-day threshold (weekly cadence, last run 2026-08-23) — cutting it close, consistent with the cron-drift concern flagged below. The no-dedup-guard and Content-Type-adjacent cron-vs-Shabbat-window concern from the static audit still stands and hasn't been touched.

---

---

## TL;DR

- The pipeline runs (or is meant to run) **~23 data streams** across 5 signal families (Curiosity, Community, Creator, Ownership, Commerce) plus one macro/context stream (Freight Index). The README only documents 8 of these — the system has grown well past its own docs.
- **Confirmed, matching what you told me:** Amazon, Kickstarter, BackerKit, DMs Guild, and DriveThruRPG are all human-bookmarklet-fed with zero Cloud Scheduler behind them. They're behind because someone has to physically click the bookmarklet — that's expected, not broken.
- **New finding:** 6 fully-built Cloud Functions — **Steam, mod.io, Nexus Mods, Twitch, AO3, D&D Beyond Catalog** — have no scheduler job, cron string, or deploy script anywhere in the repo. They may be wired up outside version control, or they may not be running on any cadence at all. One `gcloud scheduler jobs list` would settle it.
- **New finding:** Itch.io Jams has no ingestion-date column at all — there's genuinely no way to tell "stale" from "no new jams this week" for that table today.
- **New finding:** Freight Index's insert has no dedup guard (a manual re-trigger double-inserts), and its documented cron disagrees with its deployed cron in a way that may drift into the Shabbat blackout window in summer.
- I extended `stream_health_audit.py` from 8 streams to all ~23 so the next live run gives full coverage, with realistic staleness thresholds for manual vs. automated vs. cadence-unknown streams. I could not run it — see below.

---

## What I could NOT verify (original limitation — resolved later the same day, see LIVE RESULTS above)

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

## Two things worth a decision (updated post-live-check)

1. ~~Six built Cloud Functions have no scheduler evidence~~ — **RESOLVED.** Confirmed live: all six have real, enabled, Shabbat-aware Cloud Scheduler jobs. Just undocumented in the repo — a nice-to-have to capture in version control, not an operational problem.
2. **Freight Index has no dedup guard and a cron sitting close to its own staleness threshold** (8/9 days) and possibly the Shabbat blackout in summer CDT. Small fix (a `WHERE NOT EXISTS` guard + pinning the cron to an explicit UTC time well clear of 21:30–03:45 UTC) next time someone's in that file.
3. **NEW — RPGGeek is silently broken** (Content-Type misconfiguration on its Cloud Scheduler job — see LIVE RESULTS above). One-line code fix + a scheduler config update, both pending your go-ahead.
4. **NEW — Emerging Terms discovery has been finding nothing since April** due to Google Trends rate-limiting the `related_queries`/`related_topics` pytrends calls. Root-caused, but needs a real decision on approach (proxy/backoff tuning), not a quick patch.

---

## What changed in this branch

- `stream_health_audit.py`: extended from 8 streams to all ~23 identified above, with per-stream `trigger` metadata and realistic staleness thresholds — manual/bookmarklet streams get a 30-day threshold instead of the default 4 (so "haven't scraped Amazon this week" stops reading as a false alarm), and cadence-unknown Cloud Functions get 14 days instead of silently inheriting a threshold that assumes a schedule that may not exist.
- Nothing else touched. No BigQuery writes, no deploys, no scheduler changes — none of those are possible from this session anyway (no credentials), and CLAUDE.md gates them regardless.

## Next step to get real numbers

Run `python3 stream_health_audit.py` anywhere with BigQuery read access (your devcontainer with `dnd-key.json`, or Cloud Shell), or hit the live Bouncer `/system/health` endpoint directly from a browser or a non-sandboxed shell. Paste the output back and I'll turn it into the live version of this report — actual row counts, actual days-stale, actual red/green per stream.
