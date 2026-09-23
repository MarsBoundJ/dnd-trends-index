from google.cloud import bigquery
import json
from datetime import datetime

# Default staleness threshold. A stream whose newest ingest date is
# older than this many days is flagged STALE — even though the table
# still has (old) rows. This is the gap that let BackerKit sit silently
# dead for 5+ days (403'd at the edge): it had historical rows so the
# old "row_count > 0 => Healthy" check reported it Healthy.
#
# 4 days tolerates normal cadence gaps (e.g. BackerKit's Mon/Wed/Fri
# schedule has a max ~3-day Fri->Mon gap) while still catching a real
# multi-day outage. Override per-stream with "max_stale_days".
#
# IMPORTANT: freshness is only meaningful when the date column reflects
# INGESTION time. Streams whose date_col is a CONTENT date (e.g.
# YouTube published_at — a video's publish date, not when we scraped
# it) set "freshness_check": False to avoid false STALE alarms.
DEFAULT_MAX_STALE_DAYS = 4

# Threshold used for streams that are fed by a human running a browser
# bookmarklet/extension (no Cloud Scheduler at all) — Amazon, Kickstarter,
# BackerKit, DMs Guild, DriveThruRPG. There is no "cadence" to violate, so
# a short threshold just cries wolf every time nobody's manually scraped
# in a few days. 30 days still catches a genuinely abandoned stream.
MANUAL_MAX_STALE_DAYS = 30

# Cloud Functions confirmed built and callable, but with NO Cloud
# Scheduler job, cron string, or deploy-script evidence anywhere in the
# repo (see 2026-08-31 data-streams health sweep). They may be wired up
# via `gcloud` outside version control, or they may simply not be
# running on any cadence — unverified from static code alone. Run
# `gcloud scheduler jobs list --location=us-central1` to settle it.
UNVERIFIED_CADENCE_DAYS = 14


def audit_all_streams():
    client = bigquery.Client()

    streams = [
        # ---- Curiosity ---------------------------------------------------
        {
            "name": "Google Trends",
            "table_id": "dnd-trends-index.dnd_trends_categorized.trend_data_pilot",
            "date_col": "date",
            "keyword_col": "search_term",
            "trigger": "Cloud Run Job (google-trends-job) via dnd-fast-lane workflow",
        },
        {
            "name": "Wikipedia",
            "table_id": "dnd-trends-index.social_data.wikipedia_daily_views",
            "date_col": "date",
            "keyword_col": "article_title",
            "max_stale_days": 3,
            "trigger": "Cloud Run service via dnd-fast-lane workflow (SCHEDULING.md pilot stream for the fixed-time policy)",
        },
        {
            "name": "Twitch",
            "table_id": "dnd-trends-index.dnd_trends_raw.twitch_viewership",
            "date_col": "snapshot_date",
            "keyword_col": "category_name",
            "max_stale_days": UNVERIFIED_CADENCE_DAYS,
            "trigger": "HTTP Cloud Function — UNVERIFIED cadence, no scheduler artifact found in repo",
        },
        {
            "name": "Emerging Terms (discovery)",
            "table_id": "dnd-trends-index.dnd_trends_raw.emerging_terms",
            "date_col": "flagged_at",
            "keyword_col": "term",
            "max_stale_days": 9,
            "trigger": "Cloud Scheduler job discover-related-queries-weekly, managed out-of-band deliberately",
        },
        # ---- Community -----------------------------------------------------
        {
            "name": "Reddit",
            "table_id": "dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics",
            "date_col": "extraction_date",
            "keyword_col": "keyword",
            "max_stale_days": 3,
            "trigger": "Cloud Run service via dnd-fast-lane workflow",
        },
        {
            "name": "Fandom",
            # REPOINTED 2026-05-19: the old social_data.fandom_trending
            # is a DEAD legacy table (last write Jan 1) that nothing
            # writes to anymore — the live fandom_scraper /
            # fandom_view_fetcher pipeline writes here. The freshness
            # audit's first run correctly exposed the stale target
            # (it was never "Fandom dead 138d" — the audit was just
            # watching the wrong table).
            "table_id": "dnd-trends-index.dnd_trends_raw.fandom_daily_metrics",
            "date_col": "extraction_date",
            "keyword_col": "article_title",
            "trigger": "Cloud Run service via dnd-fast-lane workflow",
        },
        # ---- Creator ---------------------------------------------------
        {
            "name": "YouTube",
            # REPOINTED 2026-05-19: social_data.youtube_videos does not
            # exist (audit was 404ing). youtube_listener writes here;
            # this table is fresh (newest video same-day). NOTE: this
            # table has NO ingest timestamp — published_at is the
            # video's publish date, so freshness_check stays False.
            # TODO: add an ingested_at column to dnd_trends_raw.
            # youtube_videos so this stream gets real staleness
            # monitoring (currently only Error/empty is detectable).
            "table_id": "dnd-trends-index.dnd_trends_raw.youtube_videos",
            "date_col": "published_at",
            "keyword_col": "video_id",
            "freshness_check": False,
            "trigger": "Cloud Run service via dnd-fast-lane workflow",
        },
        {
            "name": "Itch.io Products",
            "table_id": "dnd-trends-index.dnd_trends_raw.itchio_products",
            "date_col": "collected_date",
            "keyword_col": "product_id",
            "max_stale_days": 5,
            "trigger": "Cloud Run Job via dnd-fast-lane workflow — cadence uncertain post-Caldean-retirement, verify",
        },
        {
            "name": "Itch.io Jams",
            # The jams writer (harvesters/itchio_rss/main.py) never sets
            # an ingestion-time column — start_date/end_date are the
            # jam's own content dates. There is genuinely no way to tell
            # "stale" from "no new jams this week" for this table today.
            "table_id": "dnd-trends-index.dnd_trends_raw.itchio_jams",
            "date_col": "start_date",
            "keyword_col": "jam_id",
            "freshness_check": False,
            "trigger": "Cloud Run Job via dnd-fast-lane workflow",
        },
        {
            "name": "mod.io",
            "table_id": "dnd-trends-index.dnd_trends_raw.modio_mods",
            "date_col": "fetch_date",
            "keyword_col": "mod_name",
            "max_stale_days": UNVERIFIED_CADENCE_DAYS,
            "trigger": "HTTP Cloud Function — UNVERIFIED cadence, no scheduler artifact found in repo",
        },
        {
            "name": "Nexus Mods",
            "table_id": "dnd-trends-index.dnd_trends_raw.nexus_mods",
            "date_col": "fetch_date",
            "keyword_col": "mod_name",
            "max_stale_days": UNVERIFIED_CADENCE_DAYS,
            "trigger": "HTTP Cloud Function — UNVERIFIED cadence, no scheduler artifact found in repo",
        },
        {
            "name": "AO3",
            "table_id": "dnd-trends-index.dnd_trends_raw.ao3_tag_counts",
            "date_col": "fetch_date",
            "keyword_col": "tag_name",
            "max_stale_days": UNVERIFIED_CADENCE_DAYS,
            "trigger": "HTTP Cloud Function — UNVERIFIED cadence, no scheduler artifact found in repo",
        },
        # ---- Ownership ---------------------------------------------------
        {
            "name": "BGG",
            "table_id": "dnd-trends-index.dnd_trends_raw.bgg_product_stats",
            "date_col": "date",
            "keyword_col": "concept_name",
            "trigger": "HTTP Cloud Function via dnd-fast-lane workflow ({'rpg': false}); proxy fail-closed since 2026-06-07",
        },
        {
            "name": "RPGGeek",
            "table_id": "dnd-trends-index.dnd_trends_raw.rpggeek_product_stats",
            "date_col": "date",
            "keyword_col": "concept_name",
            "trigger": "Same Cloud Function as BGG via dnd-fast-lane workflow ({'rpg': true})",
        },
        {
            "name": "Roll20",
            "table_id": "dnd-trends-index.commercial_data.roll20_rankings",
            "date_col": "snapshot_date",
            "keyword_col": "title",
            "trigger": "unknown — not present in dnd-fast-lane.yaml; no scheduler artifact found in repo",
        },
        {
            "name": "Steam",
            "table_id": "dnd-trends-index.dnd_trends_raw.steam_player_counts",
            "date_col": "snapshot_date",
            "keyword_col": "app_name",
            "max_stale_days": UNVERIFIED_CADENCE_DAYS,
            "trigger": "HTTP Cloud Function — UNVERIFIED cadence, no scheduler artifact found in repo",
        },
        # ---- Commerce ---------------------------------------------------
        {
            "name": "Kickstarter",
            "table_id": "dnd-trends-index.commercial_data.kickstarter_projects",
            "date_col": "discovered_at",
            "keyword_col": "project_id",
            "max_stale_days": MANUAL_MAX_STALE_DAYS,
            "trigger": "MANUAL — browser bookmarklet (scripts/kickstarter_bookmarklet.js), no Cloud Scheduler",
        },
        {
            "name": "BackerKit",
            "table_id": "dnd-trends-index.commercial_data.backerkit_projects",
            "date_col": "scraped_at",
            "keyword_col": "title",
            "max_stale_days": MANUAL_MAX_STALE_DAYS,
            "trigger": (
                "MANUAL — browser bookmarklet (scripts/backerkit_bookmarklet.js) since 2026-05-18; "
                "the server-side Cloud Function is dead code — deterministic 403 from BackerKit's WAF "
                "confirmed persistent on every scheduled run since >=2026-05-13"
            ),
        },
        {
            "name": "Amazon",
            "table_id": "dnd-trends-index.dnd_trends_raw.amazon_daily_stats",
            "date_col": "date",
            "keyword_col": "asin",
            "max_stale_days": MANUAL_MAX_STALE_DAYS,
            "trigger": "MANUAL — browser bookmarklet (scripts/amazon_bookmarklet.js) -> bouncer /system/amazon/ingest-ranks",
        },
        {
            "name": "DMs Guild",
            "table_id": "dnd-trends-index.dnd_trends_raw.catalog_supply",
            "date_col": "collected_date",
            "keyword_col": "title",
            "extra_where": "source = 'DMs Guild'",
            "max_stale_days": MANUAL_MAX_STALE_DAYS,
            "trigger": (
                "MANUAL — bookmarklet/Chrome extension 'Arcane Incursion' (Mon 6am + daily retry, skip Sat), "
                "but only fires if that browser + extension is actually open. Programmatic Playwright scraper "
                "was attempted and abandoned (see dtrpg_scraping_report.md — Cloudflare/JA3 fingerprinting)."
            ),
        },
        {
            "name": "DriveThruRPG",
            "table_id": "dnd-trends-index.dnd_trends_raw.catalog_supply",
            "date_col": "collected_date",
            "keyword_col": "title",
            "extra_where": "source = 'DriveThruRPG'",
            "max_stale_days": MANUAL_MAX_STALE_DAYS,
            "trigger": "MANUAL — same bookmarklet/extension path as DMs Guild (shares catalog_supply table)",
        },
        {
            "name": "D&D Beyond Catalog",
            "table_id": "dnd-trends-index.dnd_trends_raw.dndbeyond_catalog",
            "date_col": "fetch_date",
            "keyword_col": "title",
            "max_stale_days": UNVERIFIED_CADENCE_DAYS,
            "trigger": (
                "HTTP Cloud Function — UNVERIFIED cadence, no scheduler artifact found in repo. "
                "Product discovery regex-scrapes React hydration JSON — fragile to any DDB frontend change."
            ),
        },
        # ---- Context / macro (outside the 5-family model) ---------------
        {
            "name": "Freight Index",
            "table_id": "dnd-trends-index.gold_data.freight_index_daily",
            "date_col": "date",
            "keyword_col": "lane_code",
            "max_stale_days": 9,
            "trigger": (
                "Dedicated Cloud Scheduler job (weekly, Sat 22:00 CST), standalone — not in dnd-fast-lane. "
                "insert_rows() has no dedup guard (manual re-trigger double-inserts same day); the docstring's "
                "cron comment (0 3 * * 0 UTC) disagrees with the deployed Sat-22:00-CST cron, which lands "
                "inside the Shabbat blackout window in summer CDT — worth re-verifying against SCHEDULING.md."
            ),
        },
    ]

    report = []

    for stream in streams:
        try:
            table = client.get_table(stream["table_id"])

            # Simple metadata count. NOTE: for streams with "extra_where"
            # (DMs Guild / DriveThruRPG share one table) this is the WHOLE
            # table's row count, not just this source's — the per-source
            # unique_entities/latest_date below ARE correctly filtered.
            row_count = table.num_rows

            if row_count > 0:
                freshness_check = stream.get("freshness_check", True)
                max_stale = stream.get("max_stale_days", DEFAULT_MAX_STALE_DAYS)
                where_clause = (
                    f"WHERE {stream['extra_where']}" if stream.get("extra_where") else ""
                )

                # days_stale is computed server-side (works for both DATE
                # and TIMESTAMP ingest columns). Only DATE()-cast the
                # column when freshness_check is True — for content-date
                # streams (freshness_check=False) the column may be an
                # incompatible type and DATE() would error the query.
                stale_expr = (
                    f"DATE_DIFF(CURRENT_DATE(), DATE(MAX({stream['date_col']})), DAY)"
                    if freshness_check
                    else "CAST(NULL AS INT64)"
                )
                query = f"""
                SELECT
                    MAX({stream['date_col']}) as latest_date,
                    COUNT(DISTINCT {stream['keyword_col']}) as unique_entities,
                    {stale_expr} as days_stale
                FROM `{stream['table_id']}`
                {where_clause}
                """

                # Fetch results
                job = client.query(query, location=table.location)
                res = list(job)[0]

                days_stale = res.days_stale

                if (
                    freshness_check
                    and days_stale is not None
                    and days_stale > max_stale
                ):
                    # Table has rows but no fresh ingest — the silent
                    # dead-stream case (e.g. BackerKit 403'd for days).
                    status = "STALE"
                else:
                    status = "Healthy"

                report.append({
                    "Stream": stream["name"],
                    "Status": status,
                    "Rows": row_count,
                    "Latest Data": str(res.latest_date),
                    "Days Stale": days_stale,
                    "Stale Threshold": (
                        max_stale if freshness_check else "n/a (content-date)"
                    ),
                    "Unique Entities": res.unique_entities,
                    "Trigger": stream.get("trigger", "unknown"),
                })
            else:
                report.append({
                    "Stream": stream["name"],
                    "Status": "Warning/Empty",
                    "Rows": 0,
                    "Latest Data": "N/A",
                    "Days Stale": None,
                    "Unique Entities": 0,
                    "Trigger": stream.get("trigger", "unknown"),
                })

        except Exception as e:
            report.append({
                "Stream": stream["name"],
                "Status": "Error",
                "Error": str(e),
                "Trigger": stream.get("trigger", "unknown"),
            })

    # Add Article Newsroom Health
    try:
        articles_table = client.get_table("dnd-trends-index.gold_data.daily_articles")
        report.append({
            "Stream": "Daily Articles (AI)",
            "Status": "Healthy" if articles_table.num_rows > 0 else "Empty",
            "Rows": articles_table.num_rows,
            "Latest Data": "Active",
            "Unique Entities": "N/A",
        })
    except Exception:
        pass

    # Surface a non-zero exit-style summary line so a STALE/Error stream
    # is impossible to miss when this is eyeballed at the monthly retro.
    problems = [
        r for r in report
        if r.get("Status") in ("STALE", "Error", "Warning/Empty")
    ]
    # ASCII-only summary — this runs on a Windows cp1252 console where
    # emoji / em-dash raise UnicodeEncodeError (json.dumps already
    # escapes non-ASCII via the default ensure_ascii=True).
    print(json.dumps(report, indent=2))
    if problems:
        names = ", ".join(f"{p['Stream']} ({p['Status']})" for p in problems)
        print(f"\n[ATTENTION] {len(problems)} stream(s) need review: {names}")
    else:
        print("\n[OK] All streams fresh and healthy.")


if __name__ == "__main__":
    audit_all_streams()
