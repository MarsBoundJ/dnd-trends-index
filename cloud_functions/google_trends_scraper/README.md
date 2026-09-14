# google_trends_scraper

**One file collects. Read this before changing anything here.**

This directory is named like a Cloud Function and is not one. The Google Trends
data in `dnd_trends_categorized.trend_data_pilot` is written by a **Cloud Run
job**.

| Component | Runs? | What it is |
|---|---|---|
| `browser_trends.py` | **YES** | The collector. Playwright + Firefox + `playwright-stealth`, through the Webshare HTTP proxy. |
| `Dockerfile`, `entrypoint.sh` | **YES** | Build the image `gcr.io/dnd-trends-index/google-trends-scraper`. Entry: `python browser_trends.py --limit 50`. |
| `setup_scheduler.sh` | reference | Creates the Cloud Scheduler trigger. Cron `0 2 * * 0-5` — weekdays + Sunday, Shabbat skipped. |
| `run_backfill.py` | manual only | Operator tool. pytrends-based, never deployed. Deps in `requirements-backfill.txt`. |

- **Cloud Run job:** `google-trends-job` (region `us-central1`)
- **Cadence:** ~02:0x–02:2x UTC, 50 terms per run, ~1,700 rows/day
- **Scheduler:** `google-trends-daily`

## Why Playwright instead of pytrends

Google Trends answers datacenter IPs with **429** and the `/sorry/` bot page.
Reproduced Sep 14, 2026: a single pytrends call from a Cloud Function returned
`too many 429 error responses`. The Playwright build exists to get past exactly
that, which is why the proxy is mandatory — both `browser_trends.py` and the
deleted pytrends files refused to start without `PROXY_URL`.

## What was removed, Sep 14 2026, and why

`main.py` (a Cloud Function), `job.py` (a second pytrends runner) and
`watermark.py` (used only by those two) were deleted, along with the deployed
Cloud Function `google-trends-scraper`.

They were **three implementations of one collector, of which one ran.** Evidence
the function was dead:

- **2 HTTP requests in 30 days**, both `curl` from the session that investigated it
- no Cloud Scheduler job targeted its URL
- **0** references across all four Workflows
- its entry point appeared nowhere in the repo but its own definition

They had also drifted apart: different retry counts, different backoff, and
`main.py` built a proxy list and then **never passed it** to the client, so it
ran from the raw GCP IP — the precise thing its own startup guard existed to
prevent.

## Two traps worth keeping in mind

1. **Don't let a runtime deprecation notice send you here.** A Cloud Run job
   ships its own container, so managed-runtime deprecations do not apply to it.
   Check which component writes — `gcloud run jobs executions list` against the
   table's `fetched_at` — before treating a deadline as a data risk.
2. **`requirements.txt` feeds the image build.** Anything added here is
   installed into the live collector, whether or not `browser_trends.py` uses
   it. That is why the pytrends dependencies now live in a separate file.
