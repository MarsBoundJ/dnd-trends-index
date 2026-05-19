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


def audit_all_streams():
    client = bigquery.Client()

    streams = [
        {
            "name": "Google Trends",
            "table_id": "dnd-trends-index.dnd_trends_categorized.trend_data_pilot",
            "date_col": "date",
            "keyword_col": "search_term",
        },
        {
            "name": "Reddit",
            "table_id": "dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics",
            "date_col": "extraction_date",
            "keyword_col": "keyword",
            "max_stale_days": 3,  # daily stream
        },
        {
            "name": "Wikipedia",
            "table_id": "dnd-trends-index.social_data.wikipedia_daily_views",
            "date_col": "date",
            "keyword_col": "article_title",
            "max_stale_days": 3,
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
        },
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
        },
        {
            "name": "Roll20",
            "table_id": "dnd-trends-index.commercial_data.roll20_rankings",
            "date_col": "snapshot_date",
            "keyword_col": "title",
        },
        {
            "name": "Kickstarter",
            "table_id": "dnd-trends-index.commercial_data.kickstarter_projects",
            "date_col": "discovered_at",
            "keyword_col": "project_id",
        },
        {
            "name": "BackerKit",
            "table_id": "dnd-trends-index.commercial_data.backerkit_projects",
            "date_col": "scraped_at",
            "keyword_col": "title",
        },
    ]

    report = []

    for stream in streams:
        try:
            table = client.get_table(stream["table_id"])

            # Simple metadata count
            row_count = table.num_rows

            if row_count > 0:
                freshness_check = stream.get("freshness_check", True)
                max_stale = stream.get("max_stale_days", DEFAULT_MAX_STALE_DAYS)

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
                })
            else:
                report.append({
                    "Stream": stream["name"],
                    "Status": "Warning/Empty",
                    "Rows": 0,
                    "Latest Data": "N/A",
                    "Days Stale": None,
                    "Unique Entities": 0,
                })

        except Exception as e:
            report.append({
                "Stream": stream["name"],
                "Status": "Error",
                "Error": str(e),
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
